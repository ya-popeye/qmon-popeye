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

  sealed trait ValueFilterCondition

  case object All extends ValueFilterCondition

  case class Multiple(attributes: Seq[Array[Byte]]) extends ValueFilterCondition {
    require(attributes.size > 1, "must be more than one attribute")
  }

  case class Single(attribute: Array[Byte]) extends ValueFilterCondition

  val CHARSET = Charset.forName("ISO-8859-1")

  def createRowRegexp(offset: Int,
                      attrNameLength: Int,
                      attrValueLength: Int,
                      attributes: Seq[((Array[Byte], ValueFilterCondition))]): String = {
    require(attrNameLength > 0, f"attribute name length must be greater than 0, not $attrNameLength")
    require(attrValueLength > 0, f"attribute value length must be greater than 0, not $attrValueLength")
    require(attributes.nonEmpty, "attribute list is empty")
    def checkAttrNameLength(name: Array[Byte]) =
      require(name.length == attrNameLength,
        f"invalid attribute name length: expected $attrNameLength, actual ${name.length}")

    def checkAttrValueLength(value: Array[Byte]) = require(value.length == attrValueLength,
      f"invalid attribute value length: expected $attrValueLength, actual ${value.length}")

    val anyAttributeRegex = f"(?:.{${attrNameLength + attrValueLength}})*"
    val prefix = f"(?s)^.{$offset}" + anyAttributeRegex
    val suffix = anyAttributeRegex + "$"
    val infix = attributes.map {
      case (attrNameId, valueCondition) =>
        checkAttrNameLength(attrNameId)
        valueCondition match {
          case Single(attrValue) =>
            checkAttrValueLength(attrValue)
            escapeRegexp(decodeBytes(attrNameId) + decodeBytes(attrValue))
          case Multiple(attrValues) =>
            val nameRegex = escapeRegexp(decodeBytes(attrNameId))
            val attrsRegexps = attrValues.map {
              value =>
                checkAttrValueLength(value)
                escapeRegexp(decodeBytes(value))
            }
            nameRegex + attrsRegexps.mkString("(?:", "|", ")")

          case All =>
            val nameRegex = escapeRegexp(decodeBytes(attrNameId))
            nameRegex + f".{$attrValueLength}"
        }
    }.mkString(anyAttributeRegex)
    prefix + infix + suffix
  }

  private def escapeRegexp(string: String) = f"\\Q${string.replace("\\E", "\\E\\\\E\\Q")}\\E"

  private def decodeBytes(bytes: Array[Byte]) = {
    val byteBuffer = ByteBuffer.wrap(bytes)
    CHARSET.decode(byteBuffer).toString
  }
}