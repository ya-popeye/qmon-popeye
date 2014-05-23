package popeye.storage.hbase

import popeye.proto.Message
import popeye.storage.hbase.HBaseStorage.{QualifiedName, MetricKind, AttrNameKind, AttrValueKind, UniqueIdNamespaceWidth}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import popeye.Logging
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.Result
import scala.collection.mutable
import java.util.Arrays.copyOfRange
import scala.collection.immutable.SortedMap

object TsdbFormat {

  import HBaseStorage._

  val metricWidth: Int = UniqueIdMapping(MetricKind)
  val attributeNameWidth: Int = UniqueIdMapping(AttrNameKind)
  val attributeValueWidth: Int = UniqueIdMapping(AttrValueKind)
  val attributesOffset = UniqueIdNamespaceWidth + metricWidth + HBaseStorage.TIMESTAMP_BYTES
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
    val baseTime = Bytes.toInt(row, UniqueIdNamespaceWidth + metricWidth, HBaseStorage.TIMESTAMP_BYTES)
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
    val baseTime = Bytes.toInt(row, UniqueIdNamespaceWidth + metricWidth, HBaseStorage.TIMESTAMP_BYTES)
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
    val deltaShort = ((qualifier & 0xFFFF) >>> HBaseStorage.FLAG_BITS).toShort
    val floatFlag: Int = HBaseStorage.FLAG_FLOAT | 0x3.toShort
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
    val baseTime: Int = (timestamp - (timestamp % HBaseStorage.MAX_TIMESPAN)).toInt
    val rowLength = UniqueIdNamespaceWidth +
      metricWidth +
      HBaseStorage.TIMESTAMP_BYTES +
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
    val delta: Short = (point.getTimestamp % HBaseStorage.MAX_TIMESPAN).toShort
    val ndelta = delta << HBaseStorage.FLAG_BITS
    if (point.hasFloatValue)
      (Bytes.toBytes(((0xffff & (HBaseStorage.FLAG_FLOAT | 0x3)) | ndelta).toShort),
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
