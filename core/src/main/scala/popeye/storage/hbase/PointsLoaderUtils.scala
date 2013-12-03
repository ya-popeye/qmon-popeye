package popeye.storage.hbase

import java.nio.ByteBuffer
import java.nio.charset.Charset

object PointsLoaderUtils {

  val ROW_REGEX_FILTER_ENCODING = Charset.forName("ISO-8859-1")

  sealed trait ValueIdFilterCondition

  object ValueIdFilterCondition {

    case class Single(id: BytesKey) extends ValueIdFilterCondition

    case class Multiple(ids: Seq[BytesKey]) extends ValueIdFilterCondition {
      require(ids.size > 1, "must be more than one value id")
    }

    case object All extends ValueIdFilterCondition

  }

  sealed trait ValueNameFilterCondition

  object ValueNameFilterCondition {

    case class Single(name: String) extends ValueNameFilterCondition

    case class Multiple(names: Seq[String]) extends ValueNameFilterCondition {
      require(names.size > 1, "must be more than one value name")
    }

    case object All extends ValueNameFilterCondition

  }

  def createRowRegexp(offset: Int,
                      attrNameLength: Int,
                      attrValueLength: Int,
                      attributes: Map[BytesKey, ValueIdFilterCondition]): String = {
    require(attrNameLength > 0, f"attribute name length must be greater than 0, not $attrNameLength")
    require(attrValueLength > 0, f"attribute value length must be greater than 0, not $attrValueLength")
    require(attributes.nonEmpty, "attribute map is empty")
    val sortedAttributes = attributes.toList.sortBy(_._1)
    def checkAttrNameLength(name: Array[Byte]) =
      require(name.length == attrNameLength,
        f"invalid attribute name length: expected $attrNameLength, actual ${name.length}")

    def checkAttrValueLength(value: Array[Byte]) = require(value.length == attrValueLength,
      f"invalid attribute value length: expected $attrValueLength, actual ${value.length}")

    val anyNumberOfAnyAttributesRegex = f"(?:.{${attrNameLength + attrValueLength}})*"
    val prefix = f"(?s)^.{$offset}" + anyNumberOfAnyAttributesRegex
    val suffix = anyNumberOfAnyAttributesRegex + "$"
    import ValueIdFilterCondition._
    val infix = sortedAttributes.map {
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
    }.mkString(anyNumberOfAnyAttributesRegex)
    prefix + infix + suffix
  }

  private def escapeRegexp(string: String) = f"\\Q${string.replace("\\E", "\\E\\\\E\\Q")}\\E"

  private def decodeBytes(bytes: Array[Byte]) = {
    val byteBuffer = ByteBuffer.wrap(bytes)
    ROW_REGEX_FILTER_ENCODING.decode(byteBuffer).toString
  }

}
