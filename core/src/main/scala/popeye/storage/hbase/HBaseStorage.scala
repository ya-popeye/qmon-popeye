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

  /** Number of bytes on which a timestamp is encoded.  */
  final val TIMESTAMP_BYTES: Short = 4
  /** Number of LSBs in time_deltas reserved for flags.  */
  final val FLAG_BITS: Short = 4
  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  final val FLAG_FLOAT: Short = 0x8
  /** Mask to select the size of a value from the qualifier.  */
  final val LENGTH_MASK: Short = 0x7
  /** Mask to select all the FLAG_BITS.  */
  final val FLAGS_MASK: Short = (FLAG_FLOAT | LENGTH_MASK).toShort
  /** Max time delta (in seconds) we can store in a column qualifier.  */
  final val MAX_TIMESPAN: Short = 3600
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

  sealed case class ResolvedName(kind: String, name: String, id: BytesKey) {
    def this(qname: QualifiedName, id: BytesKey) = this(qname.kind, qname.name, id)

    def this(qid: QualifiedId, name: String) = this(qid.kind, name, qid.id)

    def toQualifiedName = QualifiedName(kind, name)

    def toQualifiedId = QualifiedId(kind, id)
  }

  object ResolvedName {
    def apply(qname: QualifiedName, id: BytesKey) = new ResolvedName(qname.kind, qname.name, id)

    def apply(qid: QualifiedId, name: String) = new ResolvedName(qid.kind, name, qid.id)
  }

  sealed case class QualifiedName(kind: String, name: String)

  sealed case class QualifiedId(kind: String, id: BytesKey)


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

  case class Point(timestamp: Int, value: Number)

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


  val ROW_REGEX_FILTER_ENCODING = Charset.forName("ISO-8859-1")

  sealed trait ValueIdFilterCondition {
    def isGroupByAttribute: Boolean
  }

  object ValueIdFilterCondition {

    case class Single(id: BytesKey) extends ValueIdFilterCondition {
      def isGroupByAttribute: Boolean = false
    }

    case class Multiple(ids: Seq[BytesKey]) extends ValueIdFilterCondition {
      require(ids.size > 1, "must be more than one value id")

      def isGroupByAttribute: Boolean = true
    }

    case object All extends ValueIdFilterCondition {
      def isGroupByAttribute: Boolean = true
    }

  }

  sealed trait ValueNameFilterCondition {
    def isGroupByAttribute: Boolean
  }

  object ValueNameFilterCondition {

    case class Single(name: String) extends ValueNameFilterCondition {
      def isGroupByAttribute: Boolean = false
    }

    case class Multiple(names: Seq[String]) extends ValueNameFilterCondition {
      require(names.size > 1, "must be more than one value name")

      def isGroupByAttribute: Boolean = true
    }

    case object All extends ValueNameFilterCondition {
      def isGroupByAttribute: Boolean = true
    }

  }

  def createRowRegexp(offset: Int,
                      attrNameLength: Int,
                      attrValueLength: Int,
                      attributes: Map[BytesKey, ValueIdFilterCondition]): String = {
    require(attrNameLength > 0, f"attribute name length must be greater than 0, not $attrNameLength")
    require(attrValueLength > 0, f"attribute value length must be greater than 0, not $attrValueLength")
    require(attributes.nonEmpty, "attribute map is empty")
    val sortedAttributes = attributes.toList.sortBy(_._1)
    def checkAttrNameLength(name: Array[Byte]) =
      require(name.length == attrNameLength,
        f"invalid attribute name length: expected $attrNameLength, actual ${name.length}")

    val anyNumberOfAnyAttributesRegex = f"(?:.{${attrNameLength + attrValueLength}})*"
    val prefix = f"(?s)^.{$offset}" + anyNumberOfAnyAttributesRegex
    val suffix = anyNumberOfAnyAttributesRegex + "$"
    val infix = sortedAttributes.map {
      case (attrNameId, valueCondition) =>
        checkAttrNameLength(attrNameId)
        renderAttributeRegexp(attrNameId, valueCondition, attrValueLength)
    }.mkString(anyNumberOfAnyAttributesRegex)
    prefix + infix + suffix
  }

  private def renderAttributeRegexp(attrNameId: BytesKey, valueCondition: ValueIdFilterCondition, attrValueLength: Int) = {
    import ValueIdFilterCondition._
    def checkAttrValueLength(value: Array[Byte]) = require(value.length == attrValueLength,
      f"invalid attribute value length: expected $attrValueLength, actual ${value.length}")
    valueCondition match {
      case Single(attrValue) =>
        checkAttrValueLength(attrValue)
        escapeRegexp(decodeBytes(attrNameId) + decodeBytes(attrValue))
      case Multiple(attrValues) =>
        val nameRegex = escapeRegexp(decodeBytes(attrNameId))
        val attrsRegexps = attrValues.map {
          value =>
            checkAttrValueLength(value)
            escapeRegexp(decodeBytes(value))
        }
        nameRegex + attrsRegexps.mkString("(?:", "|", ")")
      case All =>
        val nameRegex = escapeRegexp(decodeBytes(attrNameId))
        nameRegex + f".{$attrValueLength}"
    }
  }

  private def escapeRegexp(string: String) = f"\\Q${string.replace("\\E", "\\E\\\\E\\Q")}\\E"

  private def decodeBytes(bytes: Array[Byte]) = {
    val byteBuffer = ByteBuffer.wrap(bytes)
    ROW_REGEX_FILTER_ENCODING.decode(byteBuffer).toString
  }

}

class HBasePointsSink(storage: HBaseStorage)(implicit eCtx: ExecutionContext) extends PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
    storage.writePoints(points)(eCtx)
  }
}


case class HBaseStorageMetrics(name: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeProcessingTime = metrics.timer(s"$name.storage.write.processing.time")
  val writeHBaseTime = metrics.timer(s"$name.storage.write.hbase.time")
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
                   metrics: HBaseStorageMetrics,
                   resolveTimeout: Duration = 15 seconds,
                   readChunkSize: Int) extends Logging with HBaseUtils {

  val tsdbFormat = new TsdbFormat()

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
      case Single(valueName) =>
        attributeValues.resolveIdByName(valueName, create = false)(resolveTimeout)
          .map(ValueIdFilterCondition.Single)
      case Multiple(valueNames) =>
        val valueIdFutures = valueNames.map {
          valueName => attributeValues.resolveIdByName(valueName, create = false)(resolveTimeout)
        }
        Future.sequence(valueIdFutures).map(ids => ValueIdFilterCondition.Multiple(ids))
      case All => Future.successful(ValueIdFilterCondition.All)
    }
  }

  def createChunkedResults(metricId: Array[Byte],
                           timeRange: (Int, Int),
                           attributes: Map[BytesKey, ValueIdFilterCondition]) = {
    val (startTime, endTime) = timeRange
    val baseStartTime = startTime - (startTime % MAX_TIMESPAN)
    val startRow = metricId ++ Bytes.toBytes(baseStartTime)
    val stopRow = metricId ++ Bytes.toBytes(endTime)
    val scan: Scan = new Scan()
    scan.setStartRow(startRow)
    scan.setStopRow(stopRow)
    scan.addFamily(PointsFamily)
    if (attributes.nonEmpty) {
      val rowRegex = createRowRegexp(
        offset = metricNames.width + Bytes.SIZEOF_INT,
        attributeNames.width,
        attributeValues.width,
        attributes
      )
      val comparator = new RegexStringComparator(rowRegex)
      comparator.setCharset(ROW_REGEX_FILTER_ENCODING)
      scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, comparator))
    }
    getChunkedResults(tableName, readChunkSize, scan)
  }

  def getAttributesMap(attributes: Array[Byte]): PointAttributeIds = {
    val attributeWidth = attributeNames.width + attributeValues.width
    require(attributes.length % attributeWidth == 0, "bad attributes length")
    val numberOfAttributes = attributes.length / attributeWidth
    val attrNamesIndexes = (0 until numberOfAttributes).map(i => i * attributeWidth)
    val attributePairs =
      for (attrNameIndex <- attrNamesIndexes)
      yield {
        val attrValueIndex = attrNameIndex + attributeNames.width
        val nameArray = new Array[Byte](attributeNames.width)
        val valueArray = new Array[Byte](attributeValues.width)
        System.arraycopy(attributes, attrNameIndex, nameArray, 0, attributeNames.width)
        System.arraycopy(attributes, attrValueIndex, valueArray, 0, attributeValues.width)
        (new BytesKey(nameArray), new BytesKey(valueArray))
      }
    SortedMap[BytesKey, BytesKey]() ++ attributePairs
  }

  private def parseTimeValuePoints(results: Array[Result],
                                   timeRange: (Int, Int)): Seq[(PointAttributeIds, List[Point])] = {
    val pointsRows = for (result <- results) yield {
      val row = result.getRow
      val attributes = getAttributesBytes(row)
      val attributeMap = getAttributesMap(attributes.bytes)
      val points = tsdbFormat.parsePoints(result)
      (attributeMap, points): (PointAttributeIds, List[Point])
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

  def getAttributesBytes(row: Array[Byte]) = {
    val attributesLength = row.length - (metricNames.width + TIMESTAMP_BYTES)
    val attributesBuffer = new Array[Byte](attributesLength)
    System.arraycopy(row, metricNames.width + TIMESTAMP_BYTES, attributesBuffer, 0, attributesLength)
    new BytesKey(attributesBuffer)
  }

  private def parseValue(qualifierBytes: Array[Byte], valueBytes: Array[Byte]): (Short, Number) = {
    val qualifier = Bytes.toShort(qualifierBytes)
    val deltaShort = delta(qualifier)
    val floatFlag: Int = HBaseStorage.FLAG_FLOAT | 0x3.toShort
    val isFloatValue = (qualifier & floatFlag) == floatFlag
    val isIntValue = (qualifier & 0xf) == 0
    if (isFloatValue) {
      (deltaShort, Bytes.toFloat(valueBytes))
    } else if (isIntValue) {
      (deltaShort, Bytes.toLong(valueBytes))
    } else {
      throw new IllegalArgumentException("Neither int nor float values set on point")
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
    metrics.writeProcessingTime.time {
      // resolve identifiers
      // unresolved will be delayed for future expansion
      val idCache: QualifiedName => Option[BytesKey] = {
        case QualifiedName(MetricKind, name) => metricNames.findIdByName(name)
        case QualifiedName(AttrNameKind, name) => attributeNames.findIdByName(name)
        case QualifiedName(AttrValueKind, name) => attributeValues.findIdByName(name)
      }
      val (partiallyConvertedPoints, keyValues) = tsdbFormat.convertToKeyValues(points, idCache)
      val cacheMisses = partiallyConvertedPoints.unresolvedNames
      metrics.resolvedPointsMeter.mark(keyValues.size)
      metrics.delayedPointsMeter.mark(partiallyConvertedPoints.points.size)

      // write resolved points
      val writeComplete = if (!keyValues.isEmpty) Future[Int] {
        writeKv(keyValues)
        keyValues.size
      } else Future.successful[Int](0)

      val delayedKeyValuesWriteFuture =
        if (partiallyConvertedPoints.points.nonEmpty) {
          val idMapFuture = resolveNames(cacheMisses)
          idMapFuture.map {
            idMap =>
              val keyValues = partiallyConvertedPoints.convert(idMap)
              writeKv(keyValues)
              keyValues.size
          }
        } else Future.successful[Int](0)

      (delayedKeyValuesWriteFuture zip writeComplete).map {
        case (a, b) =>
          val time = ctx.stop.nano
          metrics.writeTimeMeter.mark(time.toMillis)
          (a + b).toLong
      }
    }
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
    metrics.writeHBaseTime.time {
      val hTable = hTablePool.getTable(tableName)
      hTable.setAutoFlush(false, true)
      hTable.setWriteBufferSize(4*1024*1024)
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
    }
  }

  private def mkPoint(baseTime: Int, metric: String,
                      qualifier: Short, value: Array[Byte],
                      attrs: Seq[(String, String)]): Message.Point = {
    val builder = Message.Point.newBuilder()
      .setMetric(metric)
    if ((qualifier & HBaseStorage.FLAG_FLOAT) == 0) {
      // int
      builder.setIntValue(Bytes.toLong(value))
    } else {
      // float
      builder.setFloatValue(Bytes.toFloat(value))
    }

    val deltaTs = delta(qualifier)
    builder.setTimestamp(baseTime + deltaTs)

    trace(s"Got point: ts=${builder.getTimestamp}, qualifier=$qualifier basets=$baseTime, delta=$deltaTs")

    attrs.foreach { attribute =>
      builder.addAttributesBuilder()
        .setName(attribute._1)
        .setValue(attribute._2)
    }
    builder.build
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
    val row = kv.getRow
    val kvValue = kv.getValue
    val kvQualifier = kv.getQualifier
    require(kv.getRowLength >= metricNames.width,
      s"Metric Id is wider then row key ${Bytes.toStringBinary(row)}")
    require(kv.getQualifierLength == Bytes.SIZEOF_SHORT,
      s"Expected qualifier to be with size of Short")
    val attributesLen = row.length - metricNames.width - HBaseStorage.TIMESTAMP_BYTES
    val attributeLen = attributeNames.width + attributeValues.width
    require(attributesLen %
      (attributeNames.width + attributeValues.width) == 0,
      s"Row key should have room for" +
        s" metric name (${metricNames.width} bytes) +" +
        s" timestamp (${HBaseStorage.TIMESTAMP_BYTES}} bytes)}" +
        s" qualifier (N x $attributeLen bytes)}" +
        s" but got row ${Bytes.toStringBinary(row)}")


    val metricNameFuture = metricNames.resolveNameById(util.Arrays.copyOf(kv.getRow, metricNames.width))(resolveTimeout)
    var atOffset = metricNames.width + HBaseStorage.TIMESTAMP_BYTES
    val attributes = Future.traverse((0 until attributesLen / attributeLen)
      .map { idx =>
      val atId = util.Arrays.copyOfRange(row, atOffset, atOffset + attributeNames.width)
      val atVal = util.Arrays.copyOfRange(row,
        atOffset + attributeNames.width,
        atOffset + attributeNames.width + attributeValues.width)
      atOffset += attributeLen * idx
      (atId, atVal)
    }) { attribute =>
      attributeNames.resolveNameById(attribute._1)(resolveTimeout)
        .zip(attributeValues.resolveNameById(attribute._2)(resolveTimeout))
    }
    metricNameFuture.zip(attributes).map {
      case tuple =>
        val baseTs = Bytes.toInt(row, metricNames.width, HBaseStorage.TIMESTAMP_BYTES)
        mkPoint(baseTs, tuple._1, Bytes.toShort(kvQualifier), kvValue, tuple._2)
    }
  }

  def keyValueToPoint(kv: KeyValue)(implicit eCtx: ExecutionContext): Message.Point = {
    Await.result(mkPointFuture(kv), resolveTimeout)
  }

  private def delta(qualifier: Short) = {
    ((qualifier & 0xFFFF) >>> HBaseStorage.FLAG_BITS).toShort
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

  config.actorSystem.registerOnTermination(hbase.close())

  val uniqueIdStorage = new UniqueIdStorage(config.uidsTableName, hbase.hTablePool, HBaseStorage.UniqueIdMapping)

  val storage = {
    implicit val eCtx = config.eCtx

    val uniqIdResolved = config.actorSystem.actorOf(Props.apply(UniqueIdActor(uniqueIdStorage)))
    val metrics = makeUniqueIdCache(config.uidsConfig, HBaseStorage.MetricKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    val attrNames = makeUniqueIdCache(config.uidsConfig, HBaseStorage.AttrNameKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    val attrValues = makeUniqueIdCache(config.uidsConfig, HBaseStorage.AttrValueKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    new HBaseStorage(
      config.pointsTableName, hbase.hTablePool, metrics, attrNames, attrValues,
      new HBaseStorageMetrics(config.storageName, config.metricRegistry), config.resolveTimeout, config.readChunkSize)
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
