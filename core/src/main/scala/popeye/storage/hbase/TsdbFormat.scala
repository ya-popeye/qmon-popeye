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
import java.nio.ByteBuffer
import java.nio.charset.Charset
import org.apache.hadoop.hbase.filter.{CompareFilter, RowFilter, RegexStringComparator}
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition.{AllValueNames, MultipleValueNames, SingleValueName}
import popeye.storage.hbase.HBaseStorage.ValueIdFilterCondition.{SingleValueId, MultipleValueIds, AllValueIds}
import popeye.proto.Message.Attribute

import scala.util.{Success, Failure, Try}

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
  val attributeWidth = attributeNameWidth + attributeValueWidth
  val shardIdWidth: Int = UniqueIdMapping(ShardKind)
  val valueTypeIdWidth: Int = 1

  val metricOffset = UniqueIdGenerationWidth
  val valueTypeIdOffset = metricOffset + metricWidth
  val shardIdOffset = valueTypeIdOffset + valueTypeIdWidth
  val timestampOffset = shardIdOffset + shardIdWidth
  val attributesOffset = timestampOffset + TIMESTAMP_BYTES

  val ROW_REGEX_FILTER_ENCODING = Charset.forName("ISO-8859-1")

  trait ValueType {
    def mkQualifiedValue(point: Message.Point): (Array[Byte], Array[Byte])

    def getValueTypeStructureId: Byte
  }

  object ValueTypes {

    val SingleValueTypeStructureId: Byte = 0
    val ListValueTypeStructureId: Byte = 1

    def renderQualifier(timestamp: Long, isFloat: Boolean): Array[Byte] = {
      val delta: Short = (timestamp % MAX_TIMESPAN).toShort
      val ndelta = delta << FLAG_BITS
      if (isFloat) {
        Bytes.toBytes(((0xffff & (FLAG_FLOAT | 0x3)) | ndelta).toShort)
      } else {
        Bytes.toBytes((0xffff & ndelta).toShort)
      }
    }

    def parseQualifier(qualifierBytes: Array[Byte]) = {
      require(
        qualifierBytes.length == Bytes.SIZEOF_SHORT,
        s"Expected qualifier length was ${ Bytes.SIZEOF_SHORT }, got ${ qualifierBytes.length }"
      )
      val qualifier = Bytes.toShort(qualifierBytes)
      val deltaShort = ((qualifier & 0xFFFF) >>> FLAG_BITS).toShort
      val floatFlag: Int = FLAG_FLOAT | 0x3.toShort
      val isFloatValue = (qualifier & floatFlag) == floatFlag
      val isIntValue = (qualifier & 0xf) == 0
      if (!isFloatValue && !isIntValue) {
        throw new IllegalArgumentException("Neither int nor float values set on point")
      }
      (deltaShort, isFloatValue)
    }

    def parseSingleValue(valueBytes: Array[Byte], isFloatValue: Boolean): Either[Long, Float] = {
      if (isFloatValue) {
        Right(Bytes.toFloat(valueBytes))
      } else {
        Left(Bytes.toLong(valueBytes))
      }
    }

    def parseListValue(valueBytes: Array[Byte], isFloatValue: Boolean): Either[Seq[Long], Seq[Float]] = {
      if (isFloatValue) {
        Right(FloatListValueType.parseFloatListValue(valueBytes))
      } else {
        Left(IntListValueType.parseIntListValue(valueBytes))
      }
    }

    def getType(valueType: Message.Point.ValueType): ValueType = {
      import Message.Point.ValueType._
      valueType match {
        case INT => IntValueType
        case FLOAT => FloatValueType
        case INT_LIST => IntListValueType
        case FLOAT_LIST => FloatListValueType
      }
    }
  }


  case object IntValueType extends ValueType {
    override def mkQualifiedValue(point: Message.Point): (Array[Byte], Array[Byte]) = {
      (ValueTypes.renderQualifier(point.getTimestamp, isFloat = false), Bytes.toBytes(point.getIntValue))
    }

    override def getValueTypeStructureId: Byte = ValueTypes.SingleValueTypeStructureId
  }

  case object FloatValueType extends ValueType {
    override def mkQualifiedValue(point: Message.Point): (Array[Byte], Array[Byte]) = {
      (ValueTypes.renderQualifier(point.getTimestamp, isFloat = true), Bytes.toBytes(point.getFloatValue))
    }

    override def getValueTypeStructureId: Byte = ValueTypes.SingleValueTypeStructureId
  }

  case object IntListValueType extends ValueType {
    override def mkQualifiedValue(point: Message.Point): (Array[Byte], Array[Byte]) = {
      val qualifier = ValueTypes.renderQualifier(point.getTimestamp, isFloat = false)
      val value = longsToBytes(point.getIntListValueList.asScala)
      (qualifier, value)
    }

    override def getValueTypeStructureId: Byte = ValueTypes.ListValueTypeStructureId

    def parseIntListValue(value: Array[Byte]): Array[Long] = {
      val longBuffer = ByteBuffer.wrap(value).asLongBuffer()
      val array = Array.ofDim[Long](longBuffer.remaining())
      longBuffer.get(array)
      array
    }

    private def longsToBytes(longs: Seq[java.lang.Long]) = {
      val buffer = ByteBuffer.allocate(longs.size * 8)
      for (l <- longs) {
        buffer.putLong(l.longValue())
      }
      buffer.array()
    }
  }

  case object FloatListValueType extends ValueType {
    override def mkQualifiedValue(point: Message.Point): (Array[Byte], Array[Byte]) = {
      val qualifier = ValueTypes.renderQualifier(point.getTimestamp, isFloat = true)
      val value = floatsToBytes(point.getFloatListValueList.asScala)
      (qualifier, value)
    }

    override def getValueTypeStructureId: Byte = ValueTypes.ListValueTypeStructureId

    def parseFloatListValue(value: Array[Byte]): Array[Float] = {
      val floatBuffer = ByteBuffer.wrap(value).asFloatBuffer()
      val array = Array.ofDim[Float](floatBuffer.remaining())
      floatBuffer.get(array)
      array
    }

    private def floatsToBytes(floats: Seq[java.lang.Float]) = {
      val buffer = ByteBuffer.allocate(floats.size * 4)
      for (f <- floats) {
        buffer.putFloat(f.floatValue())
      }
      buffer.array()
    }
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
      case SingleValueId(attrValue) =>
        checkAttrValueLength(attrValue)
        escapeRegexp(decodeBytes(attrNameId) + decodeBytes(attrValue))
      case MultipleValueIds(attrValues) =>
        val nameRegex = escapeRegexp(decodeBytes(attrNameId))
        val attrsRegexps = attrValues.map {
          value =>
            checkAttrValueLength(value)
            escapeRegexp(decodeBytes(value))
        }
        nameRegex + attrsRegexps.mkString("(?:", "|", ")")
      case AllValueIds =>
        val nameRegex = escapeRegexp(decodeBytes(attrNameId))
        nameRegex + f".{$attrValueLength}"
    }
  }

  private def escapeRegexp(string: String) = f"\\Q${ string.replace("\\E", "\\E\\\\E\\Q") }\\E"

  private def decodeBytes(bytes: Array[Byte]) = {
    val byteBuffer = ByteBuffer.wrap(bytes)
    ROW_REGEX_FILTER_ENCODING.decode(byteBuffer).toString
  }

  def shardAttributeToShardName(attrName: String, attrValue: String): String = {
    mutable.StringBuilder.newBuilder
      .append('{')
      .append(attrName)
      .append(": ")
      .append(attrValue)
      .append('}')
      .append('_')
      .append(attrName.length)
      .append('_')
      .append(attrValue.length)
      .toString()
  }
}

case class TimeseriesId(generationId: BytesKey,
                        metricId: BytesKey,
                        shardId: BytesKey,
                        attributeIds: SortedMap[BytesKey, BytesKey]) {
  def getMetricName(idMap: Map[QualifiedId, String]) = idMap(QualifiedId(MetricKind, generationId, metricId))

  def getAttributes(idMap: Map[QualifiedId, String]) = {
    attributeIds.map {
      case (nameId, valueId) =>
        val name = idMap(QualifiedId(AttrNameKind, generationId, nameId))
        val value = idMap(QualifiedId(AttrValueKind, generationId, valueId))
        (name, value)
    }
  }
}

case class ParsedSingleValueRowResult(timeseriesId: TimeseriesId, points: Seq[HBaseStorage.Point])

case class ParsedListValueRowResult(timeseriesId: TimeseriesId, lists: Seq[HBaseStorage.ListPoint])

sealed trait ConversionResult

case class SuccessfulConversion(keyValue: KeyValue) extends ConversionResult

case class FailedConversion(exception: Exception) extends ConversionResult

case object IdCacheMiss extends ConversionResult


class TsdbFormat(timeRangeIdMapping: GenerationIdMapping, shardAttributeNames: Set[String]) extends Logging {

  import TsdbFormat._

  def convertToKeyValue(point: Message.Point,
                        idCache: QualifiedName => Option[BytesKey],
                        currentTimeSeconds: Int): ConversionResult = {
    try {
      val generationId = getGenerationId(point, currentTimeSeconds)
      val metricId = idCache(QualifiedName(MetricKind, generationId, point.getMetric))
      val attributes = point.getAttributesList.asScala

      val shardName = getShardName(point)
      val shardId = idCache(QualifiedName(ShardKind, generationId, shardName))

      val attributeIds = attributes.map {
        attr =>
          val name = idCache(QualifiedName(AttrNameKind, generationId, attr.getName))
          val value = idCache(QualifiedName(AttrValueKind, generationId, attr.getValue))
          (name, value)
      }
      val valueType = ValueTypes.getType(point.getValueType)
      def attributesAreDefined = attributeIds.forall { case (n, v) => n.isDefined && v.isDefined }
      if (metricId.isDefined && shardId.isDefined && attributesAreDefined) {
        val qualifiedValue = valueType.mkQualifiedValue(point)
        val keyValue = mkKeyValue(
          generationId,
          metricId.get,
          valueType.getValueTypeStructureId,
          shardId.get,
          point.getTimestamp,
          attributeIds.map { case (n, v) => (n.get, v.get) },
          currentTimeSeconds.toLong * 1000,
          qualifiedValue
        )
        SuccessfulConversion(keyValue)
      } else {
        IdCacheMiss
      }
    } catch {
      case e: Exception => FailedConversion(e)
    }
  }

  private def getShardName(point: Message.Point): String = {
    val attributes = point.getAttributesList.asScala
    val shardAttributes = attributes.filter(attr => shardAttributeNames.contains(attr.getName))
    require(
      shardAttributes.size == 1,
      f"a point must have exactly one shard attribute; shard attributes: $shardAttributeNames"
    )

    val shardAttribute = shardAttributes.head
    shardAttributeToShardName(shardAttribute.getName, shardAttribute.getValue)
  }

  def getUniqueIds(timeseriesId: TimeseriesId): Seq[QualifiedId] = {
    val TimeseriesId(generationId, metricId, shardId, attributeIds) = timeseriesId
    val metricQId = QualifiedId(MetricKind, generationId, metricId)
    val shardQId = QualifiedId(ShardKind, generationId, shardId)
    val attrNameQIds = attributeIds.keys.map(id => QualifiedId(AttrNameKind, generationId, id)).toSeq
    val attrValueQIds = attributeIds.values.map(id => QualifiedId(AttrValueKind, generationId, id)).toSeq
    metricQId +: shardQId +: (attrNameQIds ++ attrValueQIds)
  }

  def parseSingleValueRowResult(result: Result): ParsedSingleValueRowResult = {
    val row = result.getRow
    val (timeseriesId, baseTime) = parseTimeseriesIdAndBaseTime(row)
    val columns = result.getFamilyMap(HBaseStorage.PointsFamily).asScala.toList
    val points = columns.map {
      case (qualifierBytes, valueBytes) =>
        val (delta, isFloat) = ValueTypes.parseQualifier(qualifierBytes)
        val value = ValueTypes.parseSingleValue(valueBytes, isFloat)
        HBaseStorage.Point(baseTime + delta, value)
    }
    ParsedSingleValueRowResult(timeseriesId, points)
  }

  def parseListValueRowResult(result: Result): ParsedListValueRowResult = {
    val row = result.getRow
    val (timeseriesId, baseTime) = parseTimeseriesIdAndBaseTime(row)
    val columns = result.getFamilyMap(HBaseStorage.PointsFamily).asScala.toList
    val listPoints = columns.map {
      case (qualifierBytes, valueBytes) =>
        val (delta, isFloat) = ValueTypes.parseQualifier(qualifierBytes)
        val value = ValueTypes.parseListValue(valueBytes, isFloat)
        HBaseStorage.ListPoint(baseTime + delta, value)
    }
    ParsedListValueRowResult(timeseriesId, listPoints)
  }

  def parseTimeseriesIdAndBaseTime(row: Array[Byte]): (TimeseriesId, Int) = {
    val attributesLength = row.length - attributesOffset
    require(
      attributesLength >= 0 && attributesLength % attributeWidth == 0,
      f"illegal row length: ${ row.length }, attributes length: $attributesLength, attr size: ${ attributeWidth }"
    )
    val generationId = new BytesKey(copyOfRange(row, 0, UniqueIdGenerationWidth))
    val metricId = new BytesKey(copyOfRange(row, metricOffset, metricOffset + metricWidth))
    val shardId = new BytesKey(copyOfRange(row, shardIdOffset, shardIdOffset + attributeValueWidth))
    val baseTime = Bytes.toInt(row, timestampOffset, TIMESTAMP_BYTES)
    val attributesBytes = copyOfRange(row, attributesOffset, row.length)
    val timeseriedId = TimeseriesId(generationId, metricId, shardId, createAttributesMap(attributesBytes))
    (timeseriedId, baseTime)
  }

  private def createAttributesMap(attributes: Array[Byte]): SortedMap[BytesKey, BytesKey] = {
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

  def getAllQualifiedNames(point: Message.Point, currentTimeInSeconds: Int): Seq[QualifiedName] = {
    val timeRangeId = getGenerationId(point, currentTimeInSeconds)
    val attributes: mutable.Buffer[Attribute] = point.getAttributesList.asScala
    val buffer = attributes.flatMap {
      attr => Seq(
        QualifiedName(AttrNameKind, timeRangeId, attr.getName),
        QualifiedName(AttrValueKind, timeRangeId, attr.getValue)
      )
    }
    buffer += QualifiedName(MetricKind, timeRangeId, point.getMetric)
    buffer += QualifiedName(ShardKind, timeRangeId, getShardName(point))
    buffer.distinct.toVector
  }

  def getScanNames(metric: String,
                   timeRange: (Int, Int),
                   attributeValueFilters: Map[String, ValueNameFilterCondition]): Set[QualifiedName] = {
    val (startTime, stopTime) = timeRange
    val generationId = getTimeRanges(startTime, stopTime).map(_.id)
    val shardNames = getShardNames(attributeValueFilters)
    generationId.flatMap {
      generationId =>
        val genIdBytes = new BytesKey(Bytes.toBytes(generationId))
        val metricName = QualifiedName(MetricKind, genIdBytes, metric)
        val shardQNames = shardNames.map(name => QualifiedName(ShardKind, genIdBytes, name))
        val attrNames = attributeValueFilters.keys.map(name => QualifiedName(AttrNameKind, genIdBytes, name)).toSeq
        val attrValues = attributeValueFilters.values.collect {
          case SingleValueName(name) => Seq(name)
          case MultipleValueNames(names) => names
        }.flatten.map(name => QualifiedName(AttrValueKind, genIdBytes, name)).toSeq
        metricName +: (shardQNames ++ attrNames ++ attrValues)
    }.toSet
  }

  private def getRowFilerOption(attributePredicates: Map[BytesKey, ValueIdFilterCondition]): Option[RowFilter] = {
    if (attributePredicates.nonEmpty) {
      val rowRegex = createRowRegexp(
        attributesOffset,
        attributeNameWidth,
        attributeValueWidth,
        attributePredicates
      )
      val comparator = new RegexStringComparator(rowRegex)
      comparator.setCharset(ROW_REGEX_FILTER_ENCODING)
      Some(new RowFilter(CompareFilter.CompareOp.EQUAL, comparator))
    } else {
      None
    }
  }

  def getScans(metric: String,
               timeRange: (Int, Int),
               attributeValueFilters: Map[String, ValueNameFilterCondition],
               idMap: Map[QualifiedName, BytesKey],
               valueTypeStructureId: Byte): Seq[Scan] = {
    val (startTime, stopTime) = timeRange
    val ranges = getTimeRanges(startTime, stopTime)
    val shardNames = getShardNames(attributeValueFilters)
    ranges.map {
      range =>
        val generationId = range.id
        val genIdBytes = new BytesKey(Bytes.toBytes(generationId))
        val shardQNames = shardNames.map(name => QualifiedName(ShardKind, genIdBytes, name))
        for {
          metricId <- idMap.get(QualifiedName(MetricKind, genIdBytes, metric))
          shardIds <- convertNamesSeq(shardQNames, idMap)
          attrIdFilters <- covertAttrNamesToIds(genIdBytes, attributeValueFilters, idMap)
        } yield {
          getShardScans(
            genIdBytes,
            metricId,
            shardIds,
            (range.start, range.stop),
            attrIdFilters,
            valueTypeStructureId)
        }
    }.collect { case Some(scans) => scans }.flatten
  }

  private def getShardNames(allFilters: Map[String, ValueNameFilterCondition]): Seq[String] = {
    val shardAttrFilterNames = allFilters.keys.filter(name => shardAttributeNames.contains(name))
    require(
      shardAttrFilterNames.size == 1,
      f"scan filters must have exactly one shard attribute; shard attributes: $shardAttributeNames"
    )
    val shardAttrName = shardAttrFilterNames.head
    val shardAttrValues = allFilters(shardAttrName) match {
      case SingleValueName(name) => Seq(name)
      case MultipleValueNames(names) => names
      case AllValueNames => throw new IllegalArgumentException("'*' filter is not supported for shard attributes")
    }
    shardAttrValues.map {
      shardAttrValue => shardAttributeToShardName(shardAttrName, shardAttrValue)
    }
  }

  private def covertAttrNamesToIds(generationId: BytesKey,
                                   attributes: Map[String, ValueNameFilterCondition],
                                   idMap: Map[QualifiedName, BytesKey]
                                    ): Option[Map[BytesKey, ValueIdFilterCondition]] = {
    val attrIdFilters = attributes.toSeq.map {
      case (attrName, valueFilter) =>
        val valueIdFilter = convertAttrValuesToIds(generationId, valueFilter, idMap).getOrElse(return None)
        val nameId = idMap.get(QualifiedName(AttrNameKind, generationId, attrName)).getOrElse(return None)
        (nameId, valueIdFilter)
    }.toMap
    Some(attrIdFilters)
  }

  private def convertAttrValuesToIds(generationId: BytesKey,
                                     value: ValueNameFilterCondition,
                                     idMap: Map[QualifiedName, BytesKey]): Option[ValueIdFilterCondition] = {
    value match {
      case SingleValueName(name) =>
        idMap.get(QualifiedName(AttrValueKind, generationId, name)).map {
          id => SingleValueId(id)
        }
      case MultipleValueNames(names) =>
        val qNames = names.map(name => QualifiedName(AttrValueKind, generationId, name))
        val idsOption = convertNamesSeq(qNames, idMap)
        idsOption.map(ids => MultipleValueIds(ids))
      case AllValueNames => Some(AllValueIds)
    }
  }

  private def convertNamesSeq(qNames: Seq[QualifiedName], idMap: Map[QualifiedName, BytesKey]): Option[Seq[BytesKey]] = {
    val idOptions = qNames.map(name => idMap.get(name))
    val ids = idOptions.collect { case Some(id) => id }
    if (ids.nonEmpty) {
      Some(ids)
    } else {
      None
    }
  }

  private def getShardScans(generationId: BytesKey,
                            metricId: BytesKey,
                            shardIds: Seq[BytesKey],
                            timeRange: (Int, Int),
                            attributePredicates: Map[BytesKey, ValueIdFilterCondition],
                            valueTypeStructureId: Byte): Seq[Scan] = {
    val (startTime, endTime) = timeRange
    val baseStartTime = startTime - (startTime % MAX_TIMESPAN)
    val rowPrefix = generationId.bytes ++ metricId.bytes :+ valueTypeStructureId
    val startTimeBytes = Bytes.toBytes(baseStartTime)
    val stopTimeBytes = Bytes.toBytes(endTime)
    for (shardId <- shardIds) yield {
      val startRow = rowPrefix ++ shardId.bytes ++ startTimeBytes
      val stopRow = rowPrefix ++ shardId.bytes ++ stopTimeBytes
      val scan = new Scan()
      scan.setStartRow(startRow)
      scan.setStopRow(stopRow)
      scan.addFamily(PointsFamily)
      getRowFilerOption(attributePredicates).foreach {
        filter => scan.setFilter(filter)
      }
      scan
    }
  }

  private def getTimeRanges(startTime: Int, stopTime: Int) = {
    val baseStartTime = getBaseTime(startTime)
    val baseStopTime = getBaseTime(stopTime)
    val ranges = timeRangeIdMapping.backwardIterator(baseStopTime)
      .takeWhile(_.stop > baseStartTime)
      .toVector
      .reverse
    if (ranges.size == 1) {
      Vector(ranges.head.copy(start = startTime, stop = stopTime))
    } else {
      ranges
        .updated(0, ranges(0).copy(start = startTime))
        .updated(ranges.length - 1, ranges(ranges.length - 1).copy(stop = stopTime))
    }
  }

  private def getGenerationId(point: Message.Point, currentTimeSeconds: Int): BytesKey = {
    val pointBaseTime = getBaseTime(point.getTimestamp.toInt)
    val currentBaseTime = getBaseTime(currentTimeSeconds)
    val id = timeRangeIdMapping.getGenerationId(pointBaseTime, currentBaseTime)
    val idBytes = new BytesKey(Bytes.toBytes(id))
    require(
      idBytes.bytes.length == UniqueIdGenerationWidth,
      f"TsdbFormat depends on generation id width: ${ idBytes.bytes.length } not equal to ${ UniqueIdGenerationWidth }"
    )
    idBytes
  }

  private def getBaseTime(timestamp: Int) = {
    timestamp - (timestamp % MAX_TIMESPAN)
  }

  private def mkKeyValue(timeRangeId: BytesKey,
                         metric: BytesKey,
                         valueTypeStructureId: Byte,
                         shardId: BytesKey,
                         timestamp: Long,
                         attributeIds: Seq[(BytesKey, BytesKey)],
                         keyValueTimestamp: Long,
                         value: (Array[Byte], Array[Byte])) = {
    val baseTime: Int = (timestamp - (timestamp % MAX_TIMESPAN)).toInt
    val rowLength = attributesOffset + attributeIds.length * attributeWidth
    val row = new Array[Byte](rowLength)
    var off = 0
    off = copyBytes(timeRangeId, row, off)
    off = copyBytes(metric, row, off)
    row(off) = valueTypeStructureId
    off += 1
    off = copyBytes(shardId, row, off)
    off = copyBytes(Bytes.toBytes(baseTime.toInt), row, off)
    val sortedAttributes = attributeIds.sortBy(_._1)
    for (attr <- sortedAttributes) {
      off = copyBytes(attr._1, row, off)
      off = copyBytes(attr._2, row, off)
    }
    val delta = (timestamp - baseTime).toShort
    trace(s"Made point: ts=$timestamp, basets=$baseTime, delta=$delta")
    new KeyValue(row, HBaseStorage.PointsFamily, value._1, keyValueTimestamp, value._2)
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
