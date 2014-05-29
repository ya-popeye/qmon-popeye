package popeye.storage.hbase

import org.apache.hadoop.hbase.util.Bytes

trait TimeRangeIdMapping {
  def getRangeId(timestampInSeconds: Int): BytesKey = backwardIterator(timestampInSeconds).next().id

  def backwardIterator(timestampInSeconds: Int): Iterator[TimeRangeAndId]
}

case class TimeRangeAndId(start: Int, stop: Int, id: BytesKey) {
  def contains(timestamp: Int) = start <= timestamp && timestamp < stop
}

class FixedTimeRangeID(id: BytesKey) extends TimeRangeIdMapping {
  override def getRangeId(timestampInSeconds: Int): BytesKey = id

  override def backwardIterator(timestampInSeconds: Int): Iterator[TimeRangeAndId] = {
    Iterator(TimeRangeAndId(Int.MinValue, Int.MaxValue, id))
  }
}

// points are packed in rows by hours, so period must be hour-aligned
class PeriodicTimeRangeId(periodInHours: Int) extends TimeRangeIdMapping {

  val periodInSeconds = periodInHours * 60 * 60 * 24
  require(HBaseStorage.UniqueIdNamespaceWidth == 2, "PeriodicTimeRangeId depends on namespace width")

  def backwardIterator(timestampInSeconds: Int): Iterator[TimeRangeAndId] = {
    val periodStartTime = timestampInSeconds - timestampInSeconds % periodInSeconds
    new TimeRangesIterator(periodStartTime)
  }

  class TimeRangesIterator(var periodStartTime: Int) extends Iterator[TimeRangeAndId] {
    override def hasNext: Boolean = periodStartTime >= 0

    override def next(): TimeRangeAndId = {
      val periodStopTime = periodStartTime + periodInSeconds
      val id = periodStartTime / periodInSeconds
      require(id <= Short.MaxValue, f"id $id is too big")
      val bytes = Array.ofDim[Byte](2)
      Bytes.putShort(bytes, 0, id.toShort)
      val idBytes = new BytesKey(bytes)
      val currentTimeRange = TimeRangeAndId(periodStartTime, periodStopTime, idBytes)
      periodStartTime -= periodInSeconds
      currentTimeRange
    }
  }

}
