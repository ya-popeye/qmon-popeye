package popeye.storage.hbase

import org.apache.hadoop.hbase.util.Bytes

trait TimeRangeIdMapping {
  def getRangeId(timestampInSeconds: Long): BytesKey
}

// points are packed in rows by hours, so period must be hour-aligned
class PeriodicTimeRangeId(periodInHours: Int) extends TimeRangeIdMapping {

  val periodInSeconds = periodInHours * 60 * 60 * 24
  require(HBaseStorage.UniqueIdNamespaceWidth == 2, "PeriodicTimeRangeId depends on namespace width")

  def getRangeId(timestampInSeconds: Long): BytesKey = {
    val id = timestampInSeconds / periodInSeconds
    require(id <= Short.MaxValue, f"id $id is too big")
    val bytes = Array.ofDim[Byte](2)
    Bytes.putShort(bytes, 0, id.toShort)
    new BytesKey(bytes)
  }
}
