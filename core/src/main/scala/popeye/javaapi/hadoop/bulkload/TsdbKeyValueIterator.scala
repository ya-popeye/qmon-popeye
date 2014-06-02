package popeye.javaapi.hadoop.bulkload

import popeye.storage.hbase.{UniqueIdStorage, TsdbFormat}
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
    val idStorage = new UniqueIdStorage(uniqueIdTableName, tablePool)
    val uniqueId = new LightweightUniqueId(idStorage, maxCacheSize)
    new TsdbKeyValueIterator(pointsIterator, uniqueId, tsdbFormat)
  }
}

class TsdbKeyValueIterator(pointsIterator: KafkaPointsIterator,
                           uniqueId: LightweightUniqueId, tsdbFormat: TsdbFormat) extends java.util.Iterator[java.util.List[KeyValue]] {

  def hasNext = pointsIterator.hasNext

  def next(): java.util.List[KeyValue] = {
    val points = pointsIterator.next()
    val (partiallyConvertedPoints, keyValues) = tsdbFormat.convertToKeyValues(points, uniqueId.findByName)
    val loadedIds = uniqueId.findOrRegisterIdsByNames(partiallyConvertedPoints.unresolvedNames)
    (partiallyConvertedPoints.convert(loadedIds) ++ keyValues).asJava
  }

  def getProgress = pointsIterator.getProgress

  override def remove(): Unit = ???
}
