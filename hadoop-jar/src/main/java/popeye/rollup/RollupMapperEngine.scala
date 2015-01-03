package popeye.rollup

import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{HTablePool, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import popeye.hadoop.bulkload.LightweightUniqueId
import popeye.proto.Message
import popeye.storage.{QualifiedId, QualifiedName}
import popeye.storage.hbase._
import popeye.util.ZkConnect
import popeye.util.hbase.HBaseConfigured
import popeye.{Logging, Point, PointRope}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedMap, mutable}

object RollupMapperOutput {
  def fromKeyValue(keyValue: KeyValue) = {
    val row = new ImmutableBytesWritable(keyValue.getRowArray, keyValue.getRowOffset, keyValue.getRowLength)
    RollupMapperOutput(row, keyValue)
  }
}

case class RollupMapperOutput(row: ImmutableBytesWritable, keyValue: KeyValue)

object RollupMapperEngine {

  val commonKeyPrefix = "popeye.rollup.RollupMapperEngine"

  val uniqueIdTableNameKey = f"$commonKeyPrefix.uniqueid.table.name"
  val uniqueIdCacheSizeKey = f"$commonKeyPrefix.uniqueid.cache.size"
  val maxDelayedRowsKey = f"$commonKeyPrefix.max.delayed.rows"
  val maxDelayedPointsKey = f"$commonKeyPrefix.max.delayed.points"
  val hBaseZkConnectStringKey = f"$commonKeyPrefix.hbase.zk.connect"
  val tsdbFormatConfigKey = f"$commonKeyPrefix.tsdbformat.config"

  def createHTablePool(conf: Configuration) = {
    val hbaseZkConnect: ZkConnect = ZkConnect.parseString(conf.get(hBaseZkConnectStringKey))
    new HBaseConfigured(ConfigFactory.empty, hbaseZkConnect).getHTablePool(2)
  }

  def createFromConfiguration(conf: Configuration, hTablePool: HTablePool): RollupMapperEngine = {
    val uniqueIdTableName: String = conf.get(uniqueIdTableNameKey)
    val uniqueIdCacheCapacity: Int = conf.getInt(uniqueIdCacheSizeKey, 100000)
    val maxDelayedRows: Int = conf.getInt(maxDelayedRowsKey, 10000)
    val maxDelayedPoints: Int = conf.getInt(maxDelayedPointsKey, 10000)
    val tsdbFormat = TsdbFormatConfig.parseString(conf.get(tsdbFormatConfigKey)).tsdbFormat

    val uniqueIdStorageMetrics = new UniqueIdStorageMetrics("uniqueid.storage", new MetricRegistry)
    val uniqueIdStorage = new UniqueIdStorage(uniqueIdTableName, hTablePool, uniqueIdStorageMetrics)
    val uniqueId = new LightweightUniqueId(uniqueIdStorage, uniqueIdCacheCapacity)
    new RollupMapperEngine(tsdbFormat, uniqueId, maxDelayedRows, maxDelayedPoints)
  }

  def setConfiguration(conf: Configuration,
                       uniqueIdTableName: String,
                       uniqueIdCacheCapacity: Int,
                       maxDelayedRows: Int,
                       maxDelayedPoints: Int,
                       hBaseZkConnect: ZkConnect,
                       tsdbFormatConfig: TsdbFormatConfig) = {
    conf.set(uniqueIdTableNameKey, uniqueIdTableName)
    conf.setInt(uniqueIdCacheSizeKey, uniqueIdCacheCapacity)
    conf.setInt(maxDelayedRowsKey, maxDelayedRows)
    conf.setInt(maxDelayedPointsKey, maxDelayedPoints)
    conf.set(hBaseZkConnectStringKey, hBaseZkConnect.toZkConnectString)
    conf.set(tsdbFormatConfigKey, TsdbFormatConfig.renderString(tsdbFormatConfig))
  }
}

class RollupMapperEngine(tsdbFormat: TsdbFormat,
                         uniqueId: LightweightUniqueId,
                         maxRowBufferSize: Int,
                         maxPointsBufferSize: Int) extends Logging {
  val rowBuffer = ArrayBuffer[ParsedSingleValueRowResult]()
  val pointsBuffer = ArrayBuffer[Message.Point]()

  def map(value: Result): java.lang.Iterable[RollupMapperOutput] = {
    val currentTimeInSeconds = (System.currentTimeMillis() / 1000).toInt
    val row = TsdbFormat.parseSingleValueRowResult(value)
    val timeseriesId = row.timeseriesId
    val nonBufferedKeyValues = findRowQIds(timeseriesId) match {
      case Some(idsMap) =>
        val metricId = timeseriesId.getMetricName(idsMap)
        val tags = timeseriesId.getAttributes(idsMap)
        val messagePoints = createRolledUpPoints(metricId, tags, row.points)
        convertToKeyValuesUsingCache(messagePoints, currentTimeInSeconds) match {
          case Some(keyValues) => keyValues
          case None =>
            pointsBuffer ++= messagePoints
            Iterable.empty
        }
      case None =>
        rowBuffer += row
        Iterable.empty
    }
    val allKeyValues =
      if (rowBuffer.size > maxRowBufferSize || pointsBuffer.size > maxPointsBufferSize) {
        val keyValuesFromRowBuffer = (bulkRollup(rowBuffer, currentTimeInSeconds) ++ nonBufferedKeyValues).toBuffer
        rowBuffer.clear()
        val keyValuesFromPointsBuffer = convertToKeyValues(pointsBuffer, currentTimeInSeconds)
        pointsBuffer.clear()
        keyValuesFromRowBuffer ++ keyValuesFromPointsBuffer ++ nonBufferedKeyValues
      } else {
        nonBufferedKeyValues
      }
    allKeyValues.map(RollupMapperOutput.fromKeyValue).asJava
  }

  def cleanup(): java.lang.Iterable[RollupMapperOutput] = {
    val currentTimeInSeconds = (System.currentTimeMillis() / 1000).toInt
    val keyValues = bulkRollup(rowBuffer, currentTimeInSeconds).map(RollupMapperOutput.fromKeyValue).toBuffer.asJava
    rowBuffer.clear()
    keyValues
  }

  def createRolledUpPoints(metric: String,
                           tags: SortedMap[String, String],
                           points: PointRope): Iterable[Message.Point] = {
    var pointsCount = 0
    var timestampsSum = 0l
    var valueSum = 0.0
    for (Point(timestamp, value) <- points.iterator) {
      pointsCount += 1
      timestampsSum += timestamp
      valueSum += value
    }
    val builder = Message.Point.newBuilder()
    builder.setMetric(metric + "_h1")
    builder.setTimestamp(timestampsSum / pointsCount)
    for ((name, value) <- tags) {
      builder.addAttributesBuilder()
        .setName(name)
        .setValue(value)
    }
    builder.setFloatValue((valueSum / pointsCount).toFloat)
    builder.setValueType(Message.Point.ValueType.FLOAT)
    Seq(builder.build())
  }

  def convertToKeyValuesUsingCache(points: Iterable[Message.Point],
                                   currentTimeInSeconds: Int): Option[Seq[KeyValue]] = {
    val keyValues = ArrayBuffer[KeyValue]()
    points.foreach {
      point => tsdbFormat.convertToKeyValue(point, uniqueId.findByName, currentTimeInSeconds) match {
        case SuccessfulConversion(keyValue) => keyValues += keyValue
        case IdCacheMiss => return None
        case FailedConversion(e) => error(f"cannot convert point: $point")
      }
    }
    Some(keyValues)
  }

  def convertToKeyValues(points: Iterable[Message.Point], currentTimeInSeconds: Int): mutable.Buffer[KeyValue] = {
    val allQNames: Set[QualifiedName] = points.flatMap {
      point => tsdbFormat.getAllQualifiedNames(point, currentTimeInSeconds)
    }(scala.collection.breakOut)
    val loadedIds = uniqueId.findOrRegisterIdsByNames(allQNames)
    val keyValues = ArrayBuffer[KeyValue]()
    points.map {
      point => tsdbFormat.convertToKeyValue(point, loadedIds.get, currentTimeInSeconds) match {
        case SuccessfulConversion(keyValue) => keyValues += keyValue
        case IdCacheMiss => error("some unique id wasn't resolved")
        case FailedConversion(ex) => error(f"cannot convert delayed point: $point")
      }
    }
    keyValues
  }

  def bulkRollup(rows: Seq[ParsedSingleValueRowResult], currentTimeIsSeconds: Int): Iterable[KeyValue] = {
    val allQIds = rows.flatMap(_.timeseriesId.getUniqueIds).toSet
    val idMap = uniqueId.resolveByIds(allQIds)
    val points = rows.flatMap {
      row =>
        try {
          val tsId = row.timeseriesId
          val metric = tsId.getMetricName(idMap)
          val tags = tsId.getAttributes(idMap)
          createRolledUpPoints(metric, tags, row.points)
        } catch {
          case t: Throwable =>
            throw t
        }
    }
    convertToKeyValues(points, currentTimeIsSeconds)
  }

  def findRowQIds(timeseriesId: TimeseriesId): Option[Map[QualifiedId, String]] = {
    val qIds = timeseriesId.getUniqueIds.toList
    val nameOptions = qIds.map(uniqueId.findById)
    if (nameOptions.exists(_.isEmpty)) {
      None
    } else {
      val names = nameOptions.map(_.get)
      Some(qIds.zip(names).toMap)
    }
  }
}