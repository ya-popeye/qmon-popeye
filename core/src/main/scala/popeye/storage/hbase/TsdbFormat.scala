package popeye.storage.hbase

import popeye.proto.Message
import popeye.storage.hbase.HBaseStorage._
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import popeye.Logging
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.{Scan, Result}
import scala.collection.mutable
import java.util.Arrays.copyOfRange
import scala.collection.immutable.SortedMap
import scala.Some
import java.nio.ByteBuffer
import java.nio.charset.Charset
import org.apache.hadoop.hbase.filter.{CompareFilter, RowFilter, RegexStringComparator}

object TsdbFormat {

  import HBaseStorage._

  /** Number of bytes on which a timestamp is encoded.  */
  final val TIMESTAMP_BYTES: Short = 4
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

  val metricWidth: Int = UniqueIdMapping(MetricKind)
  val attributeNameWidth: Int = UniqueIdMapping(AttrNameKind)
  val attributeValueWidth: Int = UniqueIdMapping(AttrValueKind)
  val attributesOffset = UniqueIdNamespaceWidth + metricWidth + TIMESTAMP_BYTES
  val ROW_REGEX_FILTER_ENCODING = Charset.forName("ISO-8859-1")


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
        f"invalid attribute name length: expected $attrNameLength, actual ${ name.length }")

    val anyNumberOfAnyAttributesRegex = f"(?:.{${ attrNameLength + attrValueLength }})*"
    val prefix = f"(?s)^.{$offset}" + anyNumberOfAnyAttributesRegex
    val suffix = anyNumberOfAnyAttributesRegex + "$"
    val infix = sortedAttributes.map {
      case (attrNameId, valueCondition) =>
        checkAttrNameLength(attrNameId)
        renderAttributeRegexp(attrNameId, valueCondition, attrValueLength)
    }.mkString(anyNumberOfAnyAttributesRegex)
    prefix + infix + suffix
  }

  private def renderAttributeRegexp(attrNameId: BytesKey, valueCondition: ValueIdFilterCondition, attrValueLength: Int) = {
    import ValueIdFilterCondition._
    def checkAttrValueLength(value: Array[Byte]) = require(value.length == attrValueLength,
      f"invalid attribute value length: expected $attrValueLength, actual ${ value.length }")
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
  }

  private def escapeRegexp(string: String) = f"\\Q${ string.replace("\\E", "\\E\\\\E\\Q") }\\E"

  private def decodeBytes(bytes: Array[Byte]) = {
    val byteBuffer = ByteBuffer.wrap(bytes)
    ROW_REGEX_FILTER_ENCODING.decode(byteBuffer).toString
  }
}


case class ParsedRowResult(namespace: BytesKey,
                           metricId: BytesKey,
                           attributes: HBaseStorage.PointAttributeIds,
                           points: Seq[HBaseStorage.Point])

class PartiallyConvertedPoints(val unresolvedNames: Set[QualifiedName],
                               val points: Seq[Message.Point],
                               resolvedNames: Map[QualifiedName, BytesKey],
                               tsdbFormat: TsdbFormat) {
  def convert(partialIdMap: Map[QualifiedName, BytesKey]) = {
    val idMap = resolvedNames.withDefault(partialIdMap)
    points.map(point => tsdbFormat.convertToKeyValue(point, idMap))
  }
}

class TsdbFormat(timeRangeIdMapping: TimeRangeIdMapping) extends Logging {

  import TsdbFormat._

  def convertToKeyValues(points: Iterable[Message.Point],
                         idCache: QualifiedName => Option[BytesKey]
                          ): (PartiallyConvertedPoints, Seq[KeyValue]) = {
    val resolvedNames = mutable.HashMap[QualifiedName, BytesKey]()
    val unresolvedNames = mutable.HashSet[QualifiedName]()
    val unconvertedPoints = mutable.ArrayBuffer[Message.Point]()
    val convertedPoints = mutable.ArrayBuffer[KeyValue]()
    for (point <- points) {
      val names = getAllQualifiedNames(point)
      val nameAndIds = names.map(name => (name, idCache(name)))
      val cacheMisses = nameAndIds.collect {case (name, None) => name}
      if (cacheMisses.isEmpty) {
        val idsMap = nameAndIds.toMap.mapValues(_.get)
        convertedPoints += convertToKeyValue(point, idsMap)
      } else {
        resolvedNames ++= nameAndIds.collect {case (name, Some(id)) => (name, id)}
        unresolvedNames ++= cacheMisses
        unconvertedPoints += point
      }
    }
    require(unresolvedNames.isEmpty == unconvertedPoints.isEmpty)
    if(unresolvedNames.isEmpty) require(resolvedNames.isEmpty)

    val partiallyConvertedPoints =
      new PartiallyConvertedPoints(unresolvedNames.toSet, unconvertedPoints.toSeq, resolvedNames.toMap, this)
    (partiallyConvertedPoints, convertedPoints.toSeq)
  }

  def convertToKeyValue(point: Message.Point, idMap: Map[QualifiedName, BytesKey]) = {
    val timeRangeId = getRangeId(point)
    val metricId = idMap(QualifiedName(MetricKind, timeRangeId, point.getMetric))
    val attributesIds = point.getAttributesList.asScala.map {
      attr =>
        val nameId = idMap(QualifiedName(AttrNameKind, timeRangeId, attr.getName))
        val valueId = idMap(QualifiedName(AttrValueKind, timeRangeId, attr.getValue))
        (nameId, valueId)
    }
    val qualifiedValue = mkQualifiedValue(point)
    mkKeyValue(
      timeRangeId,
      metricId,
      attributesIds,
      point.getTimestamp,
      qualifiedValue
    )
  }

  def parseRowResult(result: Result): ParsedRowResult = {
    val row = result.getRow
    val attributesLength = row.length - attributesOffset
    val attrSize = attributeNameWidth + attributeValueWidth
    require(
      attributesLength >= 0 && attributesLength % attrSize == 0,
      f"illegal row length: ${ row.length }, attributes length: $attributesLength, attr size: ${ attrSize }"
    )
    val namespace = new BytesKey(copyOfRange(row, 0, UniqueIdNamespaceWidth))
    val metricId = new BytesKey(copyOfRange(row, UniqueIdNamespaceWidth, UniqueIdNamespaceWidth + metricWidth))
    val baseTime = Bytes.toInt(row, UniqueIdNamespaceWidth + metricWidth, TIMESTAMP_BYTES)
    val attributesBytes = copyOfRange(row, attributesOffset, row.length)
    val columns = result.getFamilyMap(HBaseStorage.PointsFamily).asScala.toList
    val points = columns.map {
      case (qualifierBytes, valueBytes) =>
        val (delta, value) = parseValue(qualifierBytes, valueBytes)
        HBaseStorage.Point(baseTime + delta, value)
    }
    ParsedRowResult(
      namespace,
      metricId,
      createAttributesMap(attributesBytes),
      points
    )
  }

  private def createAttributesMap(attributes: Array[Byte]): HBaseStorage.PointAttributeIds = {
    val attributeWidth = attributeNameWidth + attributeValueWidth
    require(attributes.length % attributeWidth == 0, "bad attributes length")
    val numberOfAttributes = attributes.length / attributeWidth
    val attrNamesIndexes = (0 until numberOfAttributes).map(i => i * attributeWidth)
    val attributePairs =
      for (attrNameIndex <- attrNamesIndexes)
      yield {
        val attrValueIndex = attrNameIndex + attributeNameWidth
        val nameArray = new Array[Byte](attributeNameWidth)
        val valueArray = new Array[Byte](attributeValueWidth)
        System.arraycopy(attributes, attrNameIndex, nameArray, 0, attributeNameWidth)
        System.arraycopy(attributes, attrValueIndex, valueArray, 0, attributeValueWidth)
        (new BytesKey(nameArray), new BytesKey(valueArray))
      }
    SortedMap[BytesKey, BytesKey](attributePairs: _*)
  }

  def parsePoints(result: Result): List[HBaseStorage.Point] = {
    val row = result.getRow
    val baseTime = Bytes.toInt(row, UniqueIdNamespaceWidth + metricWidth, TIMESTAMP_BYTES)
    val columns = result.getFamilyMap(HBaseStorage.PointsFamily).asScala.toList
    columns.map {
      case (qualifierBytes, valueBytes) =>
        val (delta, value) = parseValue(qualifierBytes, valueBytes)
        HBaseStorage.Point(baseTime + delta, value)
    }
  }

  def getAllQualifiedNames(point: Message.Point): Seq[QualifiedName] = {
    val timeRangeId = getRangeId(point)
    val buffer = point.getAttributesList.asScala.flatMap {
      attr => Seq(
        QualifiedName(AttrNameKind, timeRangeId, attr.getName),
        QualifiedName(AttrValueKind, timeRangeId, attr.getValue)
      )
    }
    buffer += QualifiedName(MetricKind, timeRangeId, point.getMetric)
    buffer.distinct.toVector
  }

  def getScans(metricId: BytesKey,
               timeRange: (Int, Int),
               attributePredicates: Map[BytesKey, ValueIdFilterCondition]): Seq[Scan] = {
    val (startTime, endTime) = timeRange
    val baseStartTime = startTime - (startTime % MAX_TIMESPAN)
    val rowPrefix = timeRangeIdMapping.getRangeId(startTime).bytes ++ metricId.bytes
    val startRow = rowPrefix ++ Bytes.toBytes(baseStartTime)
    val stopRow = rowPrefix ++ Bytes.toBytes(endTime)
    val scan = new Scan()
    scan.setStartRow(startRow)
    scan.setStopRow(stopRow)
    scan.addFamily(PointsFamily)
    if (attributePredicates.nonEmpty) {
      val rowRegex = createRowRegexp(attributesOffset, attributeNameWidth, attributeValueWidth, attributePredicates)
      val comparator = new RegexStringComparator(rowRegex)
      comparator.setCharset(ROW_REGEX_FILTER_ENCODING)
      scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, comparator))
    }
    Seq(scan)
  }

  private def getRangeId(point: Message.Point): BytesKey = {
    val id = timeRangeIdMapping.getRangeId(point.getTimestamp)
    require(
      id.length == UniqueIdNamespaceWidth,
      f"TsdbFormat depends on namespace width: ${id.length} not equal to ${UniqueIdNamespaceWidth}"
    )
    id
  }

  private def parseValue(qualifierBytes: Array[Byte], valueBytes: Array[Byte]): (Short, Either[Long, Float]) = {
    require(
      qualifierBytes.length == Bytes.SIZEOF_SHORT,
      s"Expected qualifier length was ${ Bytes.SIZEOF_SHORT }, got ${ qualifierBytes.length }"
    )
    val qualifier = Bytes.toShort(qualifierBytes)
    val deltaShort = ((qualifier & 0xFFFF) >>> FLAG_BITS).toShort
    val floatFlag: Int = FLAG_FLOAT | 0x3.toShort
    val isFloatValue = (qualifier & floatFlag) == floatFlag
    val isIntValue = (qualifier & 0xf) == 0
    if (isFloatValue) {
      (deltaShort, Right(Bytes.toFloat(valueBytes)))
    } else if (isIntValue) {
      (deltaShort, Left(Bytes.toLong(valueBytes)))
    } else {
      throw new IllegalArgumentException("Neither int nor float values set on point")
    }
  }


  private def mkKeyValue(timeRangeId: BytesKey,
                         metric: BytesKey,
                         attrs: Seq[(BytesKey, BytesKey)],
                         timestamp: Long,
                         value: (Array[Byte], Array[Byte])) = {
    val baseTime: Int = (timestamp - (timestamp % MAX_TIMESPAN)).toInt
    val rowLength = UniqueIdNamespaceWidth +
      metricWidth +
      TIMESTAMP_BYTES +
      attrs.length * (attributeNameWidth + attributeValueWidth)
    val row = new Array[Byte](rowLength)
    var off = 0
    off = copyBytes(timeRangeId, row, off)
    off = copyBytes(metric, row, off)
    off = copyBytes(Bytes.toBytes(baseTime.toInt), row, off)
    val sortedAttributes = attrs.sortBy(_._1)
    for (attr <- sortedAttributes) {
      off = copyBytes(attr._1, row, off)
      off = copyBytes(attr._2, row, off)
    }
    val delta = (timestamp - baseTime).toShort
    trace(s"Made point: ts=$timestamp, basets=$baseTime, delta=$delta")
    new KeyValue(row, HBaseStorage.PointsFamily, value._1, value._2)
  }

  /**
   * Produce tuple with qualifier and value represented as byte arrays
   * @param point point to pack
   * @return packed (qualifier, value)
   */
  private def mkQualifiedValue(point: Message.Point): (Array[Byte], Array[Byte]) = {
    val delta: Short = (point.getTimestamp % MAX_TIMESPAN).toShort
    val ndelta = delta << FLAG_BITS
    if (point.hasFloatValue)
      (Bytes.toBytes(((0xffff & (FLAG_FLOAT | 0x3)) | ndelta).toShort),
        Bytes.toBytes(point.getFloatValue))
    else if (point.hasIntValue)
      (Bytes.toBytes((0xffff & ndelta).toShort), Bytes.toBytes(point.getIntValue))
    else
      throw new IllegalArgumentException("Neither int nor float values set on point")

  }

  @inline
  private def copyBytes(src: Array[Byte], dst: Array[Byte], off: Int): Int = {
    System.arraycopy(src, 0, dst, off, src.length)
    off + src.length
  }

}
