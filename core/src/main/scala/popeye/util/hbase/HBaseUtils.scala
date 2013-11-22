package popeye.util.hbase

import org.apache.hadoop.hbase.client.{HTablePool, HTableInterface}
import popeye.Logging
import java.nio.charset.Charset
import java.nio.ByteBuffer

private[popeye] trait HBaseUtils extends Logging {

  def hTablePool: HTablePool

  @inline
  protected def withHTable[U](tableName: String)(body: (HTableInterface) => U): U = {
    log.debug("withHTable - trying to get HTable {}", tableName)
    val hTable = hTablePool.getTable(tableName)
    log.debug("withHTable - got HTable {}", tableName)
    try {
      body(hTable)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    } finally {
      hTable.close()
    }
  }

}

private[popeye] object HBaseUtils {
  val CHARSET = Charset.forName("ISO-8859-1")

  def createRowRegexp(offset: Int, attrLength: Int, orderedAttributes: Seq[(Array[Byte], Array[Byte])]): String = {
    require(attrLength > 0, f"attribute length must be greater than 0, not $attrLength")
    require(orderedAttributes.nonEmpty, "attribute list is empty")
    for ((name, value) <- orderedAttributes) {
      require(name.length + value.length == attrLength,
        f"invalid attribute length: expected $attrLength, actual ${name.length + value.length}")
    }
    val prefix = f"(?s)^.{$offset}"
    val suffix = "$"
    val attributeDelimeter = f"(?:.{$attrLength})*"
    val infix = orderedAttributes.map {
      case (attrNameId, attrValueId) =>
        val name = escapeRegexpEscaping(decodeBytes(attrNameId))
        val value = escapeRegexpEscaping(decodeBytes(attrValueId))
        f"\\Q${name}${value}\\E"
    }.mkString(attributeDelimeter, attributeDelimeter, attributeDelimeter)
    prefix + infix + suffix
  }

  private def escapeRegexpEscaping(string:String) = string.replace("\\E", "\\E\\\\E\\Q")

  private def decodeBytes(bytes: Array[Byte]) = {
    val byteBuffer = ByteBuffer.wrap(bytes)
    CHARSET.decode(byteBuffer).toString
  }
}