package popeye.storage.hbase

import popeye.proto.Message
import popeye.storage.hbase.HBaseStorage.{QualifiedName, MetricKind, AttrNameKind, AttrValueKind}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import popeye.Logging
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.Result
import scala.collection.mutable

object TsdbFormat {
  def getAllQualifiedNames(point: Message.Point): List[QualifiedName] = {
    val buffer = point.getAttributesList.asScala.flatMap {
      attr => Seq(
        QualifiedName(AttrNameKind, attr.getName),
        QualifiedName(AttrValueKind, attr.getValue)
      )
    }
    buffer += QualifiedName(MetricKind, point.getMetric)
    buffer.distinct.toList
  }

}

class TsdbFormat(metricWidth: Int, attributeNameWidth: Int, attributeValueWidth: Int) extends Logging {

  class PartiallyConvertedPoints(val unresolvedNames: Set[QualifiedName],
                                 val points: Seq[Message.Point],
                                 resolvedNames: Map[QualifiedName, BytesKey]) {
    def convert(partialIdMap: Map[QualifiedName, BytesKey]) = {
      val idMap = resolvedNames.withDefault(partialIdMap)
      points.map(point => convertToKeyValue(point, idMap))
    }
  }

  def convertToKeyValues(points: Iterable[Message.Point],
                         idCache: QualifiedName => Option[BytesKey]
                          ): (PartiallyConvertedPoints, Seq[KeyValue]) = {
    val resolvedNames = mutable.HashMap[QualifiedName, BytesKey]()
    val unresolvedNames = mutable.HashSet[QualifiedName]()
    val unconvertedPoints = mutable.ArrayBuffer[Message.Point]()
    val convertedPoints = mutable.ArrayBuffer[KeyValue]()
    for (point <- points) {
      val names = TsdbFormat.getAllQualifiedNames(point)
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
      new PartiallyConvertedPoints(unresolvedNames.toSet, unconvertedPoints.toSeq, resolvedNames.toMap)
    (partiallyConvertedPoints, convertedPoints.toSeq)
  }

  def convertToKeyValue(point: Message.Point, idMap: Map[QualifiedName, BytesKey]) = {
    val metricId = idMap(QualifiedName(MetricKind, point.getMetric))
    val attributesIds = point.getAttributesList.asScala.map {
      attr =>
        val nameId = idMap(QualifiedName(AttrNameKind, attr.getName))
        val valueId = idMap(QualifiedName(AttrValueKind, attr.getValue))
        (nameId, valueId)
    }
    val qualifiedValue = mkQualifiedValue(point)
    mkKeyValue(metricId, attributesIds, point.getTimestamp, qualifiedValue)
  }

  def parsePoints(result: Result): List[HBaseStorage.Point] = {
    val row = result.getRow
    val baseTime = Bytes.toInt(row, metricWidth, HBaseStorage.TIMESTAMP_BYTES)
    val columns = result.getFamilyMap(HBaseStorage.PointsFamily).asScala.toList
    columns.map {
      case (qualifierBytes, valueBytes) =>
        val (delta, value) = parseValue(qualifierBytes, valueBytes)
        HBaseStorage.Point(baseTime + delta, value)
    }
  }

  private def parseValue(qualifierBytes: Array[Byte], valueBytes: Array[Byte]): (Short, Number) = {
    val qualifier = Bytes.toShort(qualifierBytes)
    val deltaShort = ((qualifier & 0xFFFF) >>> HBaseStorage.FLAG_BITS).toShort
    val floatFlag: Int = HBaseStorage.FLAG_FLOAT | 0x3.toShort
    val isFloatValue = (qualifier & floatFlag) == floatFlag
    val isIntValue = (qualifier & 0xf) == 0
    if (isFloatValue) {
      (deltaShort, Bytes.toFloat(valueBytes))
    } else if (isIntValue) {
      (deltaShort, Bytes.toLong(valueBytes))
    } else {
      throw new IllegalArgumentException("Neither int nor float values set on point")
    }
  }


  private def mkKeyValue(metric: BytesKey, attrs: Seq[(BytesKey, BytesKey)], timestamp: Long, value: (Array[Byte], Array[Byte])) = {
    val baseTime: Int = (timestamp - (timestamp % HBaseStorage.MAX_TIMESPAN)).toInt
    val row = new Array[Byte](metricWidth + HBaseStorage.TIMESTAMP_BYTES +
      attrs.length * (attributeNameWidth + attributeValueWidth))
    var off = 0
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
