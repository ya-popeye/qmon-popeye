package popeye.javaapi.hadoop.bulkload

import com.codahale.metrics.MetricRegistry
import popeye.Logging
import popeye.proto.Message
import popeye.storage.QualifiedName
import popeye.storage.hbase._
import org.apache.hadoop.hbase.KeyValue
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.HTablePool
import popeye.hadoop.bulkload.{LightweightUniqueId, KafkaPointsIterator}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TsdbKeyValueIterator {
  def create(pointsIterator: KafkaPointsIterator,
             tsdbFormat: TsdbFormat,
             uniqueIdTableName: String,
             tablePool: HTablePool,
             maxCacheSize: Int,
             maxDelayedPoints: Int) = {
    val metrics = new UniqueIdStorageMetrics("uniqueid.storage", new MetricRegistry)
    val idStorage = new UniqueIdStorage(uniqueIdTableName, tablePool, metrics)
    val uniqueId = new LightweightUniqueId(idStorage, maxCacheSize)
    new TsdbKeyValueIterator(pointsIterator, uniqueId, tsdbFormat, maxDelayedPoints)
  }
}

class TsdbKeyValueIterator(pointsIterator: KafkaPointsIterator,
                           uniqueId: LightweightUniqueId,
                           tsdbFormat: TsdbFormat,
                           maxDelayedPoints: Int) extends java.util.Iterator[java.util.List[KeyValue]] with Logging {

  val delayedPointsBuffer = mutable.Buffer[Message.Point]()

  def hasNext = pointsIterator.hasNext || delayedPointsBuffer.nonEmpty

  def next(): java.util.List[KeyValue] = {
    val currentTimeInSeconds = (System.currentTimeMillis() / 1000).toInt
    val nextKeyValues =
      if (pointsIterator.hasNext) {
        val points = pointsIterator.next()
        val (keyValues, delayedPoints) = convertToKeyValues(points, currentTimeInSeconds)
        delayedPointsBuffer ++= delayedPoints
        if (delayedPointsBuffer.size > maxDelayedPoints) {
          val delayedKeyValues = convertDelayedPoints(delayedPointsBuffer, currentTimeInSeconds)
          delayedPointsBuffer.clear()
          delayedKeyValues ++ keyValues
        } else {
          keyValues
        }
      } else {
        val keyValues = convertDelayedPoints(delayedPointsBuffer, currentTimeInSeconds)
        delayedPointsBuffer.clear()
        keyValues
      }
    nextKeyValues.asJava
  }

  def convertDelayedPoints(delayedPoints: Seq[Message.Point], currentTimeInSeconds: Int) = {
    val allQNames: Set[QualifiedName] = delayedPoints.flatMap {
      point => tsdbFormat.getAllQualifiedNames(point, currentTimeInSeconds)
    }(scala.collection.breakOut)
    val loadedIds = uniqueId.findOrRegisterIdsByNames(allQNames)
    val keyValues = ArrayBuffer[KeyValue]()
    delayedPoints.map {
      point => tsdbFormat.convertToKeyValue(point, loadedIds.get, currentTimeInSeconds) match {
        case SuccessfulConversion(keyValue) => keyValues += keyValue
        case IdCacheMiss => error("some unique id wasn't resolved")
        case FailedConversion(ex) => error(f"cannot convert delayed point: $point")
      }
    }
    keyValues
  }

  def convertToKeyValues(points: Seq[Message.Point], currentTimeInSeconds: Int) = {
    val keyValues = ArrayBuffer[KeyValue]()
    val delayedPoints = ArrayBuffer[Message.Point]()
    points.foreach {
      point => tsdbFormat.convertToKeyValue(point, uniqueId.findByName, currentTimeInSeconds) match {
        case SuccessfulConversion(keyValue) => keyValues += keyValue
        case IdCacheMiss => delayedPoints += point
        case FailedConversion(e) => error(f"cannot convert point: $point")
      }
    }
    (keyValues, delayedPoints)
  }

  def getProgress = pointsIterator.getProgress

  override def remove(): Unit = ???
}
