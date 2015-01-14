package popeye.storage.hbase

import java.io.File

import com.codahale.metrics.MetricRegistry
import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{CellUtil, KeyValue}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import popeye.proto.{Message, PackedPoints}
import popeye._
import popeye.util.ZkConnect
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Promise, Await, ExecutionContext, Future}
import com.typesafe.config.Config
import akka.actor.{Props, ActorSystem}
import java.util.concurrent.TimeUnit
import popeye.util.hbase.{HBaseUtils, HBaseConfigured}
import HBaseStorage._
import scala.collection.immutable.SortedMap
import popeye.util.hbase.HBaseUtils.ChunkedResultsMetrics
import popeye.pipeline.PointsSink
import popeye.storage.{QualifiedId, QualifiedName, ValueNameFilterCondition}

object HBaseStorage {

  type PointsGroup = Map[PointAttributes, PointRope]

  type PointAttributes = SortedMap[String, String]

  object PointsGroups {
    def empty = PointsGroups(Map.empty)

    def concatGroups(left: PointsGroup, right: PointsGroup) = {
      right.foldLeft(left) {
        case (accGroup, (attrs, newPoints)) =>
          val pointsOption = accGroup.get(attrs)
          val pointArray = pointsOption.map(oldPoints => oldPoints.concat(newPoints)).getOrElse(newPoints)
          accGroup.updated(attrs, pointArray)
      }
    }

    def concatPointsGroups(left: PointsGroups, right: PointsGroups) = {
      val newMap =
        right.groupsMap.foldLeft(left.groupsMap) {
          case (accGroups, (groupByAttrs, newGroup)) =>
            val groupOption = accGroups.get(groupByAttrs)
            val concatinatedGroups = groupOption.map {
              oldGroup => PointsGroups.concatGroups(oldGroup, newGroup)
            }.getOrElse(newGroup)
            accGroups.updated(groupByAttrs, concatinatedGroups)
        }
      PointsGroups(newMap)
    }
  }

  case class PointsGroups(groupsMap: Map[PointAttributes, PointsGroup])

  case class ListPointTimeseries(tags: SortedMap[String, String], lists: Seq[ListPoint])

  def collectAllGroups(groupsIterator: AsyncIterator[PointsGroups], cancellation: Future[Nothing] = Promise().future)
                      (implicit eCtx: ExecutionContext): Future[PointsGroups] = {
    AsyncIterator.foldLeft(
      groupsIterator,
      PointsGroups.empty,
      PointsGroups.concatPointsGroups,
      cancellation
    )
  }
}

class HBasePointsSink(storage: HBaseStorage)(implicit eCtx: ExecutionContext) extends PointsSink {

  override def sendPoints(batchId: Long, points: Message.Point*): Future[Long] = {
    storage.writeMessagePoints(points :_*)(eCtx)
  }

  override def sendPacked(batchId: Long, buffers: PackedPoints*): Future[Long] = {
    storage.writePackedPoints(buffers :_*)(eCtx)
  }

  override def close(): Unit = {}
}


case class HBaseStorageMetrics(name: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeHBaseTime = metrics.timer(s"$name.storage.write.hbase.time")
  val writeHBaseTimeMeter = metrics.meter(s"$name.storage.write.hbase.time-meter")
  val writeTime = metrics.timer(s"$name.storage.write.time")
  val writeTimeMeter = metrics.meter(s"$name.storage.write.time-meter")
  val totalWriteTimeHistogram = metrics.histogram(s"$name.storage.write.delay.time.hist")
  val writeHBasePoints = metrics.meter(s"$name.storage.write.points")
  val readProcessingTime = metrics.timer(s"$name.storage.read.processing.time")
  val resolvedPointsMeter = metrics.meter(s"$name.storage.resolved.points")
  val delayedPointsMeter = metrics.meter(s"$name.storage.delayed.points")
  val failedPointConversions = metrics.meter(s"$name.storage.failed.point.conversions")
  val chunkedResultsMetrics = new ChunkedResultsMetrics(f"$name.storage.read", metricRegistry)
}

class HBaseStorage(tableName: String,
                   hTablePool: HTablePool,
                   uniqueId: UniqueId,
                   tsdbFormat: TsdbFormat,
                   metrics: HBaseStorageMetrics,
                   resolveTimeout: Duration = 15 seconds,
                   readChunkSize: Int) extends Logging {

  val tableBytes = tableName.getBytes(TsdbFormat.Encoding)

  def getPoints(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition])
               (implicit eCtx: ExecutionContext): AsyncIterator[PointsGroups] = {
    val groupByAttributeNames =
      attributes
        .toList
        .filter { case (attrName, valueFilter) => valueFilter.isGroupByAttribute }
        .map(_._1)
    val resultsIterator = resolveQuery(metric, timeRange, attributes, TsdbFormat.ValueTypes.SingleValueTypeStructureId)
    createPointsGroupsIterator(resultsIterator, timeRange, groupByAttributeNames)
  }

  def getListPoints(metric: String,
                    timeRange: (Int, Int),
                    attributes: Map[String, ValueNameFilterCondition])
                   (implicit eCtx: ExecutionContext): AsyncIterator[Seq[ListPointTimeseries]] = {
    val resultsIterator = resolveQuery(metric, timeRange, attributes, TsdbFormat.ValueTypes.ListValueTypeStructureId)
    def resultsToListPointsTimeseries(results: Array[Result]): Future[Seq[ListPointTimeseries]] = {
      val rowResults = results.map(TsdbFormat.parseListValueRowResult)
      val ids = rowResults.flatMap(rr => tsdbFormat.getUniqueIds(rr.timeseriesId)).toSet
      val idNamePairsFuture = Future.traverse(ids) {
        case qId =>
          uniqueId.resolveNameById(qId)(resolveTimeout).map(name => (qId, name))
      }
      idNamePairsFuture.map {
        idNamePairs =>
          val idMap = idNamePairs.toMap
          toListPointSequences(rowResults, timeRange, idMap)
      }
    }
    resultsIterator.map(resultsToListPointsTimeseries)
  }

  private def resolveQuery(metric: String,
                           timeRange: (Int, Int),
                           attributes: Map[String, ValueNameFilterCondition],
                           valueTypeStructureId: Byte)
                          (implicit eCtx: ExecutionContext): AsyncIterator[Array[Result]] = {
    val scanNames = tsdbFormat.getScanNames(metric, timeRange, attributes)
    val scanNameIdPairsFuture = Future.traverse(scanNames) {
      qName =>
        uniqueId.resolveIdByName(qName, create = false)(resolveTimeout)
          .map(id => Some(qName, id))
          .recover { case e: NoSuchElementException => None }
    }
    val resutlsIteratorFuture = for {
      scanNameIdPairs <- scanNameIdPairsFuture
    } yield {
      val scanNameToIdMap = scanNameIdPairs.collect { case Some(x) => x }.toMap
      val scans = tsdbFormat.getScans(
        metric,
        timeRange,
        attributes,
        scanNameToIdMap,
        valueTypeStructureId
      )
      val scansString = scans.map {
        scan =>
          val startRow = Bytes.toStringBinary(scan.getStartRow)
          val stopRow = Bytes.toStringBinary(scan.getStopRow)
          s"start row = $startRow stop row = $stopRow"
      }.mkString("\n")
      debug(s"starting hbase scans:\n$scansString")
      val resultsIterator = HBaseUtils.getChunkedResults(
        metrics.chunkedResultsMetrics,
        hTablePool,
        tableName,
        readChunkSize,
        scans
      )
      AsyncIterator.fromImmutableIterator(resultsIterator)
    }
    AsyncIterator.unwrapFuture(resutlsIteratorFuture)
  }

  def createPointsGroupsIterator(chunkedResults: AsyncIterator[Array[Result]],
                                 timeRange: (Int, Int),
                                 groupByAttributeNameIds: Seq[String])
                                (implicit eCtx: ExecutionContext): AsyncIterator[PointsGroups] = {

    def resultsToPointsGroups(results: Array[Result]): Future[PointsGroups] = {
      val rowResults = results.map(TsdbFormat.parseSingleValueRowResult)
      val ids = rowResults.flatMap(rr => tsdbFormat.getUniqueIds(rr.timeseriesId)).toSet
      val idNamePairsFuture = Future.traverse(ids) {
        case qId =>
          uniqueId.resolveNameById(qId)(resolveTimeout).map(name => (qId, name))
      }
      idNamePairsFuture.map {
        idNamePairs =>
          val idMap = idNamePairs.toMap
          val pointSequences: Map[PointAttributes, PointRope] = toPointSequencesMap(rowResults, timeRange, idMap)
          val pointGroups: Map[PointAttributes, PointsGroup] = groupPoints(groupByAttributeNameIds, pointSequences)
          PointsGroups(pointGroups)
      }
    }
    chunkedResults.map(resultsToPointsGroups)
  }

  private def toPointSequencesMap(rows: Array[ParsedSingleValueRowResult],
                                  timeRange: (Int, Int),
                                  idMap: Map[QualifiedId, String]): Map[PointAttributes, PointRope] = {
    val (startTime, endTime) = timeRange
    rows.groupBy(row => row.timeseriesId.getAttributes(idMap)).mapValues {
      rowsArray =>
        val pointsSeq = rowsArray.to[mutable.IndexedSeq].map(_.points)
        val firstRow = pointsSeq(0)
        pointsSeq(0) = firstRow.filter(point => point.timestamp >= startTime)
        val lastIndex = pointsSeq.length - 1
        val lastRow = pointsSeq(lastIndex)
        pointsSeq(lastIndex) = lastRow.filter(point => point.timestamp < endTime)
        PointRope.concatAll(pointsSeq)
    }.view.force // mapValues returns lazy Map
  }

  private def toListPointSequences(rows: Array[ParsedListValueRowResult],
                                   timeRange: (Int, Int),
                                   idMap: Map[QualifiedId, String]): Seq[ListPointTimeseries] = {
    val (startTime, endTime) = timeRange
    val serieses = rows.groupBy(row => row.timeseriesId).mapValues {
      rowsArray =>
        val pointsArray = rowsArray.map(_.lists)
        val firstRow = pointsArray(0)
        pointsArray(0) = firstRow.filter(list => list.timestamp >= startTime)
        val lastIndex = pointsArray.length - 1
        val lastRow = pointsArray(lastIndex)
        pointsArray(lastIndex) = lastRow.filter(point => point.timestamp < endTime)
        pointsArray.toSeq.flatten
    }
    serieses.map {
      case (timeseriesId, lists) =>
        val tags = timeseriesId.getAttributes(idMap)
        ListPointTimeseries(tags, lists)
    }.toSeq
  }

  def groupPoints(groupByAttributeNames: Seq[String],
                  pointsSequences: Map[PointAttributes, PointRope]): Map[PointAttributes, PointsGroup] = {
    pointsSequences.groupBy {
      case (attributes, _) =>
        val groupByAttributeValueIds = groupByAttributeNames.map(attributes(_))
        SortedMap[String, String](groupByAttributeNames zip groupByAttributeValueIds: _*)
    }
  }

  def ping(): Unit = {
    val qName = QualifiedName(TsdbFormat.MetricKind, new BytesKey(Array[Byte](0, 0)), "_.ping")
    val future = uniqueId.resolveIdByName(qName, create = true)(resolveTimeout)
    Await.result(future, resolveTimeout)
  }

  def writePackedPoints(packed: PackedPoints*)(implicit eCtx: ExecutionContext): Future[Long] = {
    // view wrapper is used to concatenate Iterables lazily
    val nonStrictCollection: Iterable[Iterable[Message.Point]] = packed.view
    writePoints(nonStrictCollection.flatten)
  }

  def writeMessagePoints(points: Message.Point*)(implicit eCtx: ExecutionContext): Future[Long] = {
    writePoints(points)
  }

  /**
   * Write points, returned future is awaitable, but can be already completed
   * @param points what to write
   * @return number of written points
   */
  def writePoints(points: Iterable[Message.Point])(implicit eCtx: ExecutionContext): Future[Long] = {

    val ctx = metrics.writeTime.timerContext()
    // resolve identifiers
    // unresolved will be delayed for future expansion
    val pointsBuffer = points.toBuffer
    val pointTimestamps = pointsBuffer.map(_.getTimestamp)
    val currentTimeInSeconds = (System.currentTimeMillis() / 1000).toInt
    convertToKeyValues(pointsBuffer, currentTimeInSeconds).flatMap {
      case (keyValues, delayedPoints) =>
        // write resolved points
        val writeComplete =
          if (keyValues.nonEmpty) {
            Future[Int] {
              writeKv(keyValues)
              keyValues.size
            }
          } else {
            Future.successful[Int](0)
          }

        val delayedKeyValuesWriteFuture = writeDelayedPoints(delayedPoints, currentTimeInSeconds)

        (delayedKeyValuesWriteFuture zip writeComplete).map {
          case (a, b) =>
            val time = ctx.stop.nano
            metrics.writeTimeMeter.mark(time.toMillis)
            val currentTimeInSeconds = (System.currentTimeMillis() / 1000).toInt
            for (timestamp <- pointTimestamps) {
              metrics.totalWriteTimeHistogram.update(currentTimeInSeconds - timestamp)
            }
            (a + b).toLong
        }
    }
  }

  private def writeDelayedPoints(delayedPoints: Seq[Message.Point], currentTimeInSeconds: Int)
                                (implicit eCtx: ExecutionContext): Future[Int] = {
    if (delayedPoints.nonEmpty) {
      val names = delayedPoints.flatMap(point => tsdbFormat.getAllQualifiedNames(point, currentTimeInSeconds)).toSet
      val idMapFuture = resolveNames(names)
      idMapFuture.map {
        idMap =>
          val keyValues = ArrayBuffer[KeyValue]()
          delayedPoints.map(point => tsdbFormat.convertToKeyValue(point, idMap.get, currentTimeInSeconds)).foreach {
            case SuccessfulConversion(keyValue) => keyValues += keyValue
            case IdCacheMiss => handlePointConversionError(
              new RuntimeException("delayed points conversion error: not all names were resolved")
            )
            case FailedConversion(ex) => handlePointConversionError(ex)
          }
          writeKv(keyValues)
          keyValues.size
      }
    } else Future.successful[Int](0)
  }

  private def convertToKeyValues(points: Iterable[Message.Point], currentTimeInSeconds: Int)
                                (implicit eCtx: ExecutionContext): Future[(Seq[KeyValue], Seq[Message.Point])] =
    Future {
      val idCache: QualifiedName => Option[BytesKey] = uniqueId.findIdByName
      val keyValues = ArrayBuffer[KeyValue]()
      val delayedPoints = ArrayBuffer[Message.Point]()
      points.foreach {
        point => tsdbFormat.convertToKeyValue(point, idCache, currentTimeInSeconds) match {
          case SuccessfulConversion(keyValue) => keyValues += keyValue
          case IdCacheMiss => delayedPoints += point
          case FailedConversion(e) => handlePointConversionError(e)
        }
      }
      metrics.resolvedPointsMeter.mark(keyValues.size)
      metrics.delayedPointsMeter.mark(delayedPoints.size)
      (keyValues, delayedPoints)
    }

  private def handlePointConversionError(e: Exception): Unit = {
    error("Point -> KeyValue conversion failed", e)
    metrics.failedPointConversions.mark()
  }

  private def resolveNames(names: Set[QualifiedName])(implicit eCtx: ExecutionContext): Future[Map[QualifiedName, BytesKey]] = {
    val namesSeq = names.toSeq
    val idsFuture = Future.traverse(namesSeq) {
      qName => uniqueId.resolveIdByName(qName, create = true)(resolveTimeout)
    }
    idsFuture.map {
      ids => namesSeq.zip(ids)(scala.collection.breakOut): Map[QualifiedName, BytesKey]
    }
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
    val timeNano = timer.stop()
    val timeMillis = TimeUnit.NANOSECONDS.toMillis(timeNano)
    metrics.writeHBaseTimeMeter.mark(timeMillis)
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
    val (timeseriesId, baseTime) = TsdbFormat.parseTimeseriesIdAndBaseTime(CellUtil.cloneRow(kv))
    val qualifierBytes = CellUtil.cloneQualifier(kv)
    val valueBytes = CellUtil.cloneValue(kv)
    val (delta, isFloat) = TsdbFormat.ValueTypes.parseQualifier(qualifierBytes)
    val value = TsdbFormat.ValueTypes.parseSingleValue(valueBytes, isFloat)
    val timestamp = baseTime + delta
    val rowIds = tsdbFormat.getUniqueIds(timeseriesId)
    val idNamePairsFuture = Future.traverse(rowIds) {
      case qId =>
        uniqueId.resolveNameById(qId)(resolveTimeout).map {
          name => (qId, name)
        }
    }
    idNamePairsFuture.map {
      idNamePairs =>
        val metricName = timeseriesId.getMetricName(idNamePairs.toMap)
        val attrs = timeseriesId.getAttributes(idNamePairs.toMap)
        val builder = Message.Point.newBuilder()
        builder.setTimestamp(timestamp)
        builder.setMetric(metricName)
        value.fold(
          longValue => {
            builder.setIntValue(longValue)
            builder.setValueType(Message.Point.ValueType.INT)
          },
          floatValue => {
            builder.setFloatValue(floatValue)
            builder.setValueType(Message.Point.ValueType.FLOAT)
          }
        )
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

object HBaseStorageConfig {
  def apply(config: Config, shardAttributeNames: Set[String], storageName: String = "hbase"): HBaseStorageConfig = {
    import scala.collection.JavaConverters._
    val uidsTableName = config.getString("tables.uids.name")
    val pointsTableName = config.getString("tables.points.name")
    def hadoopConfiguration = {
      val conf = new Configuration()
      for (path <- config.getStringList("hadoop.conf.paths").asScala) {
        conf.addResource(new File(path).toURI.toURL)
      }
      conf
    }
    val pointsTableCoprocessorJarPathOption = {
      val coprocessorJarKey = "tables.points.coprocessor.jar.path"
      if (config.hasPath(coprocessorJarKey)) {
        val hdfs = FileSystem.newInstance(hadoopConfiguration)
        val jarPath = try {
          val pathString = config.getString(coprocessorJarKey)
          hdfs.makeQualified(new Path(pathString))
        }
        Some(jarPath)
      } else {
        None
      }
    }
    val poolSize = config.getInt("pool.max")
    val zkQuorum = ZkConnect.parseString(config.getString("zk.quorum"))
    val uidsConfig = config.getConfig("uids")
    val resolveTimeout = new FiniteDuration(uidsConfig.getMilliseconds(s"resolve-timeout"), TimeUnit.MILLISECONDS)
    val readChunkSize = config.getInt("read-chunk-size")
    val tsdbFormatConfig = {
      val startTimeAndPeriods = StartTimeAndPeriod.parseConfigList(config.getConfigList("generations"))
      TsdbFormatConfig(startTimeAndPeriods, shardAttributeNames)
    }
    val uidsCacheInitialCapacity = uidsConfig.getInt("cache.initial-capacity")
    val uidsCacheMaxCapacity = uidsConfig.getInt("cache.max-capacity")

    HBaseStorageConfig(
      config,
      hadoopConfiguration,
      uidsTableName,
      pointsTableName,
      pointsTableCoprocessorJarPathOption,
      poolSize,
      zkQuorum,
      readChunkSize,
      resolveTimeout,
      uidsCacheInitialCapacity,
      uidsCacheMaxCapacity,
      tsdbFormatConfig,
      storageName
    )
  }
}

case class HBaseStorageConfig(hbaseConfig: Config,
                              private val hadoopConfigurationInner: Configuration,
                              uidsTableName: String,
                              pointsTableName: String,
                              pointsTableCoprocessorJarPathOption: Option[Path],
                              poolSize: Int,
                              zkQuorum: ZkConnect,
                              readChunkSize: Int,
                              resolveTimeout: FiniteDuration,
                              uidsCacheInitialCapacity: Int,
                              uidsCacheMaxCapacity: Int,
                              tsdbFormatConfig: TsdbFormatConfig,
                              storageName: String) {
  def hadoopConfiguration = new Configuration(hadoopConfigurationInner)
}

/**
 * Encapsulates configured hbase client and points storage actors.
 * @param config provides necessary configuration parameters
 */
class HBaseStorageConfigured(config: HBaseStorageConfig, actorSystem: ActorSystem, metricRegistry: MetricRegistry)
                            (implicit val eCtx: ExecutionContext) extends Logging {

  info(f"initializing HBaseStorage, config: $config")

  val hbase = new HBaseConfigured(config.hbaseConfig, config.zkQuorum)
  val hTablePool: HTablePool = hbase.getHTablePool(config.poolSize)

  TsdbTables.createTables(
    hbase.hbaseConfiguration,
    config.pointsTableName,
    config.uidsTableName,
    config.pointsTableCoprocessorJarPathOption
  )

  actorSystem.registerOnTermination(hTablePool.close())

  val uniqueIdStorage = {
    val metrics = new UniqueIdStorageMetrics("uniqueid.storage", metricRegistry)
    new UniqueIdStorage(config.uidsTableName, hTablePool, metrics)
  }

  val storage = {
    val uniqIdResolver = actorSystem.actorOf(Props.apply(UniqueIdActor(uniqueIdStorage, actorSystem.dispatcher)))
    val uniqueId = new UniqueIdImpl(
      uniqIdResolver,
      new UniqueIdMetrics("uniqueid", metricRegistry),
      config.uidsCacheInitialCapacity,
      config.uidsCacheMaxCapacity,
      config.resolveTimeout
    )
    val metrics: HBaseStorageMetrics = new HBaseStorageMetrics(config.storageName, metricRegistry)
    val tsdbFormat = config.tsdbFormatConfig.tsdbFormat
    new HBaseStorage(
      config.pointsTableName,
      hTablePool,
      uniqueId,
      tsdbFormat,
      metrics,
      config.resolveTimeout,
      config.readChunkSize)
  }
}
