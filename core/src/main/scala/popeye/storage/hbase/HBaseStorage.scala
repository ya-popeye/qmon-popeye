package popeye.storage.hbase

import java.nio.charset.Charset

/**
 * @author Andrey Stepachev
 */
object HBaseStorage {
  final val Encoding = Charset.forName("UTF-8")

  /** Number of bytes on which a timestamp is encoded.  */
  final val TIMESTAMP_BYTES: Short = 4
  /** Maximum number of tags allowed per data point.  */
  final val MAX_NUM_TAGS: Short = 8
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

  final val MetricKind: String = "metric"
  final val AttrNameKind: String = "tagk"
  final val AttrValueKind: String = "tagv"

  final val UniqueIdMapping = Map[String, Short](
    MetricKind -> 3.toShort,
    AttrNameKind -> 3.toShort,
    AttrValueKind -> 3.toShort
  )
}

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




