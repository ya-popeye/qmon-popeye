package popeye.hadoop.bulkload

import popeye.storage.hbase.{UniqueIdStorage, TsdbFormat}
import org.apache.hadoop.hbase.KeyValue
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.HTablePool

object TsdbKeyValueIterator {
  def create(pointsIterator: KafkaPointsIterator, uniqueIdTableName: String, tablePool: HTablePool, maxCacheSize: Int) = {
    val idStorage = new UniqueIdStorage(uniqueIdTableName, tablePool)
    val uniqueId = new LightweightUniqueId(idStorage, maxCacheSize)
    new TsdbKeyValueIterator(pointsIterator, uniqueId)
  }
}

class TsdbKeyValueIterator(pointsIterator: KafkaPointsIterator, uniqueId: LightweightUniqueId) {

  val tsdbFormat = new TsdbFormat

  def hasNext = pointsIterator.hasNext

  def next(): java.util.List[KeyValue] = {
    val points = pointsIterator.next()
    val (partiallyConvertedPoints, keyValues) = tsdbFormat.convertToKeyValues(points, uniqueId.findByName)
    val loadedIds = uniqueId.findOrRegisterIdsByNames(partiallyConvertedPoints.unresolvedNames)
    (partiallyConvertedPoints.convert(loadedIds) ++ keyValues).asJava
  }

  def getProgress = pointsIterator.getProgress
}
