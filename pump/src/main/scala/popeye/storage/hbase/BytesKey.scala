package popeye.storage.hbase

import org.apache.hadoop.hbase.util.Bytes

object BytesKey {
  implicit def bytesToBytesKey(bytes: Array[Byte]) = new BytesKey(bytes)
  implicit def bytesKeyToBytes(bkey: BytesKey) = bkey.bytes

}

/**
 * Helper class, making byte[] comparable
 * @param bytes what to wrap
 */
class BytesKey(val bytes: Array[Byte]) extends Comparable[BytesKey] {

  def compareTo(other: BytesKey): Int = {
    Bytes.BYTES_COMPARATOR.compare(this.bytes, other.asInstanceOf[BytesKey].bytes)
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other: BytesKey =>
        compareTo(other) == 0
      case _ =>
        throw new IllegalArgumentException("Object of wrong class compared: " + obj.getClass)
    }
  }

  override def hashCode(): Int = {
    Bytes.hashCode(bytes)
  }

  override def toString: String = Bytes.toStringBinary(bytes)
}

