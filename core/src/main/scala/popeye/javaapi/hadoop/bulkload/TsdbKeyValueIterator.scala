package popeye.javaapi.hadoop.bulkload

import com.codahale.metrics.MetricRegistry
import popeye.storage.hbase.{UniqueIdStorageMetrics, UniqueIdStorage, TsdbFormat}
import org.apache.hadoop.hbase.KeyValue
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.HTablePool
import popeye.hadoop.bulkload.{LightweightUniqueId, KafkaPointsIterator}

object TsdbKeyValueIterator {
  def create(pointsIterator: KafkaPointsIterator,
             tsdbFormat: TsdbFormat,
             uniqueIdTableName: String,
             tablePool: HTablePool,
             maxCacheSize: Int) = {
    val metrics = new UniqueIdStorageMetrics("uniqueid.storage", new MetricRegistry)
    val idStorage = new UniqueIdStorage(uniqueIdTableName, tablePool, metrics)
    val uniqueId = new LightweightUniqueId(idStorage, maxCacheSize)
    new TsdbKeyValueIterator(pointsIterator, uniqueId, tsdbFormat)
  }
}

class TsdbKeyValueIterator(pointsIterator: KafkaPointsIterator,
                           uniqueId: LightweightUniqueId, tsdbFormat: TsdbFormat) extends java.util.Iterator[java.util.List[KeyValue]] {

  def hasNext = pointsIterator.hasNext

  def next(): java.util.List[KeyValue] = {
    val points = pointsIterator.next()
    val currentTimeInSeconds = (System.currentTimeMillis() / 1000).toInt
    ???
  }

  def getProgress = pointsIterator.getProgress

  override def remove(): Unit = ???
}
