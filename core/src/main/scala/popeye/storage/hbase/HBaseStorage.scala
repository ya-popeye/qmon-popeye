package popeye.storage.hbase

import com.codahale.metrics.MetricRegistry
import java.util
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import popeye.proto.{PackedPoints, Message}
import popeye.{Instrumented, Logging}
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import com.typesafe.config.Config
import akka.actor.{ActorRef, Props, ActorSystem}
import java.util.concurrent.TimeUnit
import popeye.util.hbase.{HBaseUtils, HBaseConfigured}
import popeye.pipeline.PointsSink
import java.nio.charset.Charset
import org.apache.hadoop.hbase.filter.{RegexStringComparator, CompareFilter, RowFilter}
import java.nio.ByteBuffer
import HBaseStorage._
import scala.collection.immutable.SortedMap
import popeye.util.hbase.HBaseUtils.ChunkedResults

object HBaseStorage {
  final val Encoding = Charset.forName("UTF-8")

  /**
   * Array containing the hexadecimal characters (0 to 9, A to F).
   * This array is read-only, changing its contents leads to an undefined
   * behavior.
   */
  final val HEX: Array[Byte] = Array[Byte]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

  final val PointsFamily = "t".getBytes(Encoding)

  final val MetricKind: String = "metric"
  final val AttrNameKind: String = "tagk"
  final val AttrValueKind: String = "tagv"

  final val UniqueIdMapping = Map[String, Short](
    MetricKind -> 3.toShort,
    AttrNameKind -> 3.toShort,
    AttrValueKind -> 3.toShort
  )

  final val UniqueIdNamespaceWidth = 2

  sealed case class ResolvedName(kind: String, namespace: BytesKey, name: String, id: BytesKey) {
    def this(qname: QualifiedName, id: BytesKey) = this(qname.kind, qname.namespace, qname.name, id)

    def this(qid: QualifiedId, name: String) = this(qid.kind, qid.namespace, name, qid.id)

    def toQualifiedName = QualifiedName(kind, namespace, name)

    def toQualifiedId = QualifiedId(kind, namespace, id)
  }

  object ResolvedName {
    val defaultNamespace = new BytesKey(Array())

    def apply(qname: QualifiedName, id: BytesKey) = new ResolvedName(qname.kind, qname.namespace, qname.name, id)

    def apply(qid: QualifiedId, name: String) = new ResolvedName(qid.kind, qid.namespace, name, qid.id)

    def apply(kind: String, name: String, id: BytesKey) = new ResolvedName(kind, defaultNamespace, name, id)
  }

  object QualifiedName {

    def apply(kind: String, name: String): QualifiedName = QualifiedName(kind, ResolvedName.defaultNamespace, name)
  }

  sealed case class QualifiedName(kind: String, namespace: BytesKey, name: String)

  object QualifiedId {

    def apply(kind: String, id: BytesKey): QualifiedId = QualifiedId(kind, ResolvedName.defaultNamespace, id)
  }

  sealed case class QualifiedId(kind: String, namespace: BytesKey, id: BytesKey)


  type NamedPointsGroup = Map[PointAttributes, Seq[Point]]

  type PointsGroup = Map[PointAttributeIds, Seq[Point]]

  type PointAttributes = SortedMap[String, String]

  type PointAttributeIds = SortedMap[BytesKey, BytesKey]

  def concatGroups(a: NamedPointsGroup, b: NamedPointsGroup) = {
    b.foldLeft(a) {
      case (accGroup, (attrs, newPoints)) =>
        val pointsOption = accGroup.get(attrs)
        accGroup.updated(attrs, pointsOption.getOrElse(Seq()) ++ newPoints)
    }
  }

  case class PointsGroups(groupsMap: Map[PointAttributes, NamedPointsGroup]) {
    def concat(that: PointsGroups) = {
      val newMap =
        that.groupsMap.foldLeft(groupsMap) {
          case (accGroups, (groupByAttrs, newGroup)) =>
            val groupOption = accGroups.get(groupByAttrs)
            val concatinatedGroups = groupOption.map(oldGroup => concatGroups(oldGroup, newGroup)).getOrElse(newGroup)
            accGroups.updated(groupByAttrs, concatinatedGroups)
        }
      PointsGroups(newMap)
    }
  }

  object Point {
    def apply(timestamp: Int, value: Long): Point = Point(timestamp, Left(value))
  }

  case class Point(timestamp: Int, value: Either[Long, Float]) {
    def isFloat = value.isRight

    def getFloatValue = value match { case Right(f) => f }

    def getLongValue = value match { case Left(l) => l }

    def doubleValue = {
      if (isFloat) {
        getFloatValue.toDouble
      } else {
        getLongValue.toDouble
      }
    }
  }

  case class PointsStream(groups: PointsGroups, next: Option[() => Future[PointsStream]]) {
    def toFuturePointsGroups(implicit eCtx: ExecutionContext): Future[PointsGroups] = {
      val nextPointsFuture = next match {
        case Some(nextFuture) => nextFuture().flatMap(_.toFuturePointsGroups)
        case None => Future.successful(PointsGroups(Map()))
      }
      nextPointsFuture.map {
        nextGroups => groups.concat(nextGroups)
      }
    }

  }

  object PointsStream {
    def apply(groups: PointsGroups): PointsStream = PointsStream(groups, None)

    def apply(groups: PointsGroups, nextStream: => Future[PointsStream]): PointsStream =
      PointsStream(groups, Some(() => nextStream))
  }

  sealed trait ValueIdFilterCondition {
    def isGroupByAttribute: Boolean
  }

  object ValueIdFilterCondition {

    case class SingleValueId(id: BytesKey) extends ValueIdFilterCondition {
      def isGroupByAttribute: Boolean = false
    }

    case class MultipleValueIds(ids: Seq[BytesKey]) extends ValueIdFilterCondition {
      require(ids.size > 1, "must be more than one value id")

      def isGroupByAttribute: Boolean = true
    }

    case object AllValueIds extends ValueIdFilterCondition {
      def isGroupByAttribute: Boolean = true
    }

  }

  sealed trait ValueNameFilterCondition {
    def isGroupByAttribute: Boolean
  }

  object ValueNameFilterCondition {

    case class SingleValueName(name: String) extends ValueNameFilterCondition {
      def isGroupByAttribute: Boolean = false
    }

    case class MultipleValueNames(names: Seq[String]) extends ValueNameFilterCondition {
      require(names.size > 1, "must be more than one value name")

      def isGroupByAttribute: Boolean = true
    }

    case object AllValueNames extends ValueNameFilterCondition {
      def isGroupByAttribute: Boolean = true
    }
  }

}

class HBasePointsSink(storage: HBaseStorage)(implicit eCtx: ExecutionContext) extends PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
    storage.writePoints(points)(eCtx)
  }
}


case class HBaseStorageMetrics(name: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeHBaseTime = metrics.timer(s"$name.storage.write.hbase.time")
  val writeHBaseTimeMeter = metrics.meter(s"$name.storage.write.hbase.time-meter")
  val writeTime = metrics.timer(s"$name.storage.write.time")
  val writeTimeMeter = metrics.meter(s"$name.storage.write.time-meter")
  val writeHBasePoints = metrics.meter(s"$name.storage.write.points")
  val readProcessingTime = metrics.timer(s"$name.storage.read.processing.time")
  val readHBaseTime = metrics.timer(s"$name.storage.read.hbase.time")
  val resolvedPointsMeter = metrics.meter(s"$name.storage.resolved.points")
  val delayedPointsMeter = metrics.meter(s"$name.storage.delayed.points")
}

class HBaseStorage(tableName: String,
                   hTablePool_ : HTablePool,
                   metricNames: UniqueId,
                   attributeNames: UniqueId,
                   attributeValues: UniqueId,
                   tsdbFormat: TsdbFormat,
                   metrics: HBaseStorageMetrics,
                   resolveTimeout: Duration = 15 seconds,
                   readChunkSize: Int) extends Logging with HBaseUtils {

  def hTablePool: HTablePool = hTablePool_

  val tableBytes = tableName.getBytes(Encoding)

  def getPoints(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition])(implicit eCtx: ExecutionContext): Future[PointsStream] = {
    val attrFilterFutures = attributes.toList.map {
      case (name, valueFilterCondition) =>
        val nameIdFuture = attributeNames.resolveIdByName(name, create = false)(resolveTimeout)
        val valueIdFilterFuture = resolveFilterCondition(valueFilterCondition)
        nameIdFuture zip valueIdFilterFuture
    }
    val futurePointsStream =
      for {
        metricId <- metricNames.resolveIdByName(metric, create = false)(resolveTimeout)
        attrValueFilters <- Future.sequence(attrFilterFutures)
      } yield {
        val attrValueFiltersMap = attrValueFilters.toMap
        val pointRowsQuery = createChunkedResults(metricId, timeRange, attrValueFiltersMap)
        val groupByAttributeNameIds =
          attrValueFiltersMap
            .toList
            .filter { case (attrName, valueFilter) => valueFilter.isGroupByAttribute}
            .map(_._1)
        createPointsStream(pointRowsQuery, timeRange, groupByAttributeNameIds)
      }
    futurePointsStream.flatMap(future => future)
  }

  def createPointsStream(chunkedResults: ChunkedResults,
                         timeRange: (Int, Int),
                         groupByAttributeNameIds: Seq[BytesKey])
                        (implicit eCtx: ExecutionContext): Future[PointsStream] = {

    def toPointsStream(chunkedResults: ChunkedResults): Future[PointsStream] = {
      val (results, nextResults) = chunkedResults.getRows()
      val pointsRows: Seq[(PointAttributeIds, Seq[Point])] = parseTimeValuePoints(results, timeRange)
      val pointGroups: Map[PointAttributeIds, PointsGroup] = groupPoints(groupByAttributeNameIds, pointsRows)
      val nextStream = nextResults.map {
        query => () => toPointsStream(query)
      }
      val (nameIds, valueIds) = allAttributeIds(pointGroups)
      val attrNameNamesFutures = nameIds.map(id => attributeNames.resolveNameById(id)(resolveTimeout))
      val attrValueNamesFutures = valueIds.map(id => attributeValues.resolveNameById(id)(resolveTimeout))
      for {
        attributeNameNames <- Future.sequence(attrNameNamesFutures)
        attributeValueNames <- Future.sequence(attrValueNamesFutures)
      } yield {
        val attrNameIdToNameMap = (nameIds zip attributeNameNames).toMap
        val attrValueIdToNameMap = (valueIds zip attributeValueNames).toMap
        val points = toNamedPointsGroups(pointGroups, attrNameIdToNameMap, attrValueIdToNameMap)
        PointsStream(PointsGroups(points), nextStream)
      }
    }
    toPointsStream(chunkedResults)
  }

  private def toNamedPointsGroups(groups: Map[PointAttributeIds, PointsGroup],
                                  names: Map[BytesKey, String],
                                  values: Map[BytesKey, String]) = {
    def nameAttributes(attrsIds: PointAttributeIds) = attrsIds.map {
      case (nameId, valueId) => (names(nameId), values(valueId))
    }
    groups.map {
      case (groupByAttributes, group) =>
        val namedGroupByAttributes = nameAttributes(groupByAttributes)
        val namedAttributeGroup = group.map {
          case (attributes, pointsSeq) =>
            val namedAttributes = nameAttributes(attributes)
            (namedAttributes, pointsSeq)
        }
        (namedGroupByAttributes, namedAttributeGroup)
    }
  }

  private def allAttributeIds(groups: Map[PointAttributeIds, PointsGroup]): (Seq[BytesKey], Seq[BytesKey]) = {
    val groupByAttrs = groups.keys.map(_.toList).flatten
    val otherAttrs = groups.values.map(_.keys.map(_.toList).flatten).flatten
    val allAttrs = groupByAttrs ++ otherAttrs
    val nameIds = allAttrs.map(_._1).toList.distinct
    val valueIds = allAttrs.map(_._2).toList.distinct
    (nameIds, valueIds)
  }

  def resolveFilterCondition(nameCondition: ValueNameFilterCondition)
                            (implicit eCtx: ExecutionContext): Future[ValueIdFilterCondition] = {
    import ValueNameFilterCondition._
    nameCondition match {
      case SingleValueName(valueName) =>
        attributeValues.resolveIdByName(valueName, create = false)(resolveTimeout)
          .map(ValueIdFilterCondition.SingleValueId)
      case MultipleValueNames(valueNames) =>
        val valueIdFutures = valueNames.map {
          valueName => attributeValues.resolveIdByName(valueName, create = false)(resolveTimeout)
        }
        Future.sequence(valueIdFutures).map(ids => ValueIdFilterCondition.MultipleValueIds(ids))
      case AllValueNames => Future.successful(ValueIdFilterCondition.AllValueIds)
    }
  }

  def createChunkedResults(metricId: Array[Byte],
                           timeRange: (Int, Int),
                           attributes: Map[BytesKey, ValueIdFilterCondition]) = {
    val scan = ??? //tsdbFormat.getScans(metricId, timeRange, attributes).head
    getChunkedResults(tableName, readChunkSize, scan)
  }

  private def parseTimeValuePoints(results: Array[Result],
                                   timeRange: (Int, Int)): Seq[(PointAttributeIds, Seq[Point])] = {
    val pointsRows = for (result <- results) yield {
      val parsedResult = tsdbFormat.parseRowResult(result)
      (parsedResult.attributes, parsedResult.points): (PointAttributeIds, Seq[Point])
    }
    val (startTime, endTime) = timeRange
    if (pointsRows.nonEmpty) {
      val firstRow = pointsRows(0)
      pointsRows(0) = firstRow.copy(_2 = firstRow._2.filter(point => point.timestamp >= startTime))
      val lastIndex = pointsRows.length - 1
      val lastRow = pointsRows(lastIndex)
      pointsRows(lastIndex) = lastRow.copy(_2 = lastRow._2.filter(point => point.timestamp < endTime))
    }
    pointsRows
  }

  def groupPoints(groupByAttributeNameIds: Seq[BytesKey],
                  pointsRows: Seq[(PointAttributeIds, Seq[Point])]): Map[PointAttributeIds, PointsGroup] = {
    val groupedRows = pointsRows.groupBy {
      case (attributes, _) =>
        val groupByAttributeValueIds = groupByAttributeNameIds.map(attributes(_))
        SortedMap[BytesKey, BytesKey]() ++ (groupByAttributeNameIds zip groupByAttributeValueIds)
    }
    groupedRows.mapValues {
      case rows => rows
        .groupBy { case (attributes, points) => attributes}
        .mapValues(_.flatMap { case (attributes, points) => points}): PointsGroup
    }
  }

  def ping(): Unit = {
    Await.result(metricNames.resolveIdByName("_.ping", create = true)(resolveTimeout), resolveTimeout)
  }
  /**
   * Write points, returned future is awaitable, but can be already completed
   * @param points what to write
   * @return number of written points
   */
  def writePoints(points: PackedPoints)(implicit eCtx: ExecutionContext): Future[Long] = {

    val ctx = metrics.writeTime.timerContext()
      // resolve identifiers
      // unresolved will be delayed for future expansion
    convertToKeyValues(points).flatMap {
      case (partiallyConvertedPoints, keyValues) =>
        // write resolved points
        val writeComplete = if (!keyValues.isEmpty) Future[Int] {
          keyValues.size
        } else Future.successful[Int](0)
        writeKv(keyValues)

        val delayedKeyValuesWriteFuture = writePartiallyConvertedPoints(partiallyConvertedPoints)

        (delayedKeyValuesWriteFuture zip writeComplete).map {
          case (a, b) =>
            val time = ctx.stop.nano
            metrics.writeTimeMeter.mark(time.toMillis)
            (a + b).toLong
        }
    }
  }

  private def writePartiallyConvertedPoints(pPoints: PartiallyConvertedPoints)
                                           (implicit eCtx: ExecutionContext): Future[Int] = {
    if (pPoints.points.nonEmpty) {
      val idMapFuture = resolveNames(pPoints.unresolvedNames)
      idMapFuture.map {
        idMap =>
          val keyValues = pPoints.convert(idMap)
          writeKv(keyValues)
          keyValues.size
      }
    } else Future.successful[Int](0)
  }

  private def convertToKeyValues(points: PackedPoints)
                                (implicit eCtx: ExecutionContext)
  : Future[(PartiallyConvertedPoints, Seq[KeyValue])] = Future {
    val idCache: QualifiedName => Option[BytesKey] = {
      case QualifiedName(MetricKind, _, name) => metricNames.findIdByName(name)
      case QualifiedName(AttrNameKind, _, name) => attributeNames.findIdByName(name)
      case QualifiedName(AttrValueKind, _, name) => attributeValues.findIdByName(name)
    }
    val result@(partiallyConvertedPoints, keyValues) = tsdbFormat.convertToKeyValues(points, idCache)
    metrics.resolvedPointsMeter.mark(keyValues.size)
    metrics.delayedPointsMeter.mark(partiallyConvertedPoints.points.size)
    result
  }

  private def resolveNames(names: Set[QualifiedName])(implicit eCtx: ExecutionContext): Future[Map[QualifiedName, BytesKey]] = {
    val groupedNames = names.groupBy(_.kind)
    def idsMapFuture(qualifiedNames: Set[QualifiedName], uniqueId: UniqueId): Future[Map[QualifiedName, BytesKey]] = {
      val namesSeq = qualifiedNames.toSeq
      val idsFuture = Future.traverse(namesSeq.map(_.name)) {
        name => uniqueId.resolveIdByName(name, create = true)(resolveTimeout)
      }
      idsFuture.map {
        ids => namesSeq.zip(ids)(scala.collection.breakOut)
      }
    }
    val metricsMapFuture = idsMapFuture(groupedNames.getOrElse(MetricKind, Set()), metricNames)
    val attrNamesMapFuture = idsMapFuture(groupedNames.getOrElse(AttrNameKind, Set()), attributeNames)
    val attrValueMapFuture = idsMapFuture(groupedNames.getOrElse(AttrValueKind, Set()), attributeValues)
    for {
      metricsMap <- metricsMapFuture
      attrNameMap <- attrNamesMapFuture
      attrValueMap <- attrValueMapFuture
    } yield metricsMap ++ attrNameMap ++ attrValueMap
  }

  private def writeKv(kvList: Seq[KeyValue]) = {
    debug(s"Making puts for ${kvList.size} keyvalues")
    val puts = new util.ArrayList[Put](kvList.length)
    kvList.foreach {
      k =>
        puts.add(new Put(k.getRow).add(k))
    }
    withDebug {
      val l = puts.map(_.heapSize()).foldLeft(0l)(_ + _)
      debug(s"Writing ${kvList.size} keyvalues (heapsize=$l)")
    }
    val timer = metrics.writeHBaseTime.timerContext()
    val hTable = hTablePool.getTable(tableName)
    hTable.setAutoFlush(false, true)
    hTable.setWriteBufferSize(4 * 1024 * 1024)
    try {
      hTable.batch(puts)
      debug(s"Writing ${kvList.size} keyvalues - flushing")
      hTable.flushCommits()
      debug(s"Writing ${kvList.size} keyvalues - done")
      metrics.writeHBasePoints.mark(puts.size())
    } catch {
      case e: Exception =>
        error("Failed to write points", e)
        throw e
    } finally {
      hTable.close()
    }
    val time = timer.stop()
    metrics.writeHBaseTimeMeter.mark(time)
  }

  /**
   * Makes Future for Message.Point from KeyValue.
   * Most time all identifiers are cached, so this future returns 'complete',
   * but in case of some unresolved identifiers future will be incomplete and become asynchronous
   *
   * @param kv keyvalue to restore
   * @return future
   */
  private def mkPointFuture(kv: KeyValue)(implicit eCtx: ExecutionContext): Future[Message.Point] = {
    import scala.collection.JavaConverters._
    val parsedResult = tsdbFormat.parseRowResult(new Result(Seq(kv).asJava))
    val attrByteIds = parsedResult.attributes.toSeq
    val metricNameFuture = metricNames.resolveNameById(parsedResult.metricId)(resolveTimeout)
    val attributes = Future.traverse(attrByteIds) {
      case (nameId, valueId) =>
        attributeNames.resolveNameById(nameId)(resolveTimeout)
          .zip(attributeValues.resolveNameById(valueId)(resolveTimeout))
    }
    val valuePoint = parsedResult.points.head
    metricNameFuture.zip(attributes).map {
      case (metricName, attrs) =>
        val builder = Message.Point.newBuilder()
        builder.setTimestamp(valuePoint.timestamp)
        builder.setMetric(metricName)
        if (valuePoint.isFloat) {
          builder.setFloatValue(valuePoint.getFloatValue)
        } else {
          builder.setIntValue(valuePoint.getLongValue)
        }
        for ((name, value) <- attrs) {
          builder.addAttributesBuilder()
            .setName(name)
            .setValue(value)
        }
        builder.build()
    }
  }

  def keyValueToPoint(kv: KeyValue)(implicit eCtx: ExecutionContext): Message.Point = {
    Await.result(mkPointFuture(kv), resolveTimeout)
  }

}

class HBaseStorageConfig(val config: Config,
                          val actorSystem: ActorSystem,
                          val metricRegistry: MetricRegistry,
                          val storageName: String = "hbase")
                         (implicit val eCtx: ExecutionContext){
  val uidsTableName = config.getString("table.uids")
  val pointsTableName = config.getString("table.points")
  val poolSize = config.getInt("pool.max")
  val zkQuorum = config.getString("zk.quorum")
  val resolveTimeout = new FiniteDuration(config.getMilliseconds(s"uids.resolve-timeout"), TimeUnit.MILLISECONDS)
  val readChunkSize = config.getInt("read-chunk-size")
  val uidsConfig = config.getConfig("uids")
}

/**
 * Encapsulates configured hbase client and points storage actors.
 * @param config provides necessary configuration parameters
 */
class HBaseStorageConfigured(config: HBaseStorageConfig) {

  val hbase = new HBaseConfigured(
    config.config, config.zkQuorum, config.poolSize)

  CreateTsdbTables.createTables(hbase.hbaseConfiguration, config.pointsTableName, config.uidsTableName)

  config.actorSystem.registerOnTermination(hbase.close())

  val uniqueIdStorage = new UniqueIdStorage(config.uidsTableName, hbase.hTablePool)

  val storage = {
    implicit val eCtx = config.eCtx

    val uniqIdResolved = config.actorSystem.actorOf(Props.apply(UniqueIdActor(uniqueIdStorage)))
    val metrics = makeUniqueIdCache(config.uidsConfig, HBaseStorage.MetricKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    val attrNames = makeUniqueIdCache(config.uidsConfig, HBaseStorage.AttrNameKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    val attrValues = makeUniqueIdCache(config.uidsConfig, HBaseStorage.AttrValueKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    val tsdbFormat = new TsdbFormat(new FixedTimeRangeID(new BytesKey(Array[Byte](0, 0))))
    new HBaseStorage(
      config.pointsTableName,
      hbase.hTablePool,
      metrics,
      attrNames,
      attrValues,
      tsdbFormat,
      new HBaseStorageMetrics(config.storageName, config.metricRegistry),
      config.resolveTimeout,
      config.readChunkSize)
  }

  private def makeUniqueIdCache(config: Config, kind: String, resolver: ActorRef,
                                storage: UniqueIdStorage, resolveTimeout: FiniteDuration)
                               (implicit eCtx: ExecutionContext): UniqueId = {
    new UniqueIdImpl(storage.kindWidth(kind), kind, resolver,
      config.getInt(s"$kind.initial-capacity"),
      config.getInt(s"$kind.max-capacity"),
      resolveTimeout
    )
  }

}
