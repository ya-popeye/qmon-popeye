package popeye.storage.hbase

import org.scalatest.{Matchers, FlatSpec}
import popeye.{ListPoint, Point}
import popeye.proto.Message
import scala.collection.JavaConverters._
import popeye.storage.QualifiedName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import popeye.test.PopeyeTestUtils._
import scala.collection.immutable.SortedMap
import org.apache.hadoop.hbase.{Cell, CellUtil, KeyValue}
import java.nio.CharBuffer
import java.util.regex.Pattern
import scala.util.Random
import popeye.storage.ValueIdFilterCondition.{AllValueIds, SingleValueId, MultipleValueIds}
import popeye.storage.ValueNameFilterCondition.{AllValueNames, SingleValueName, MultipleValueNames}
import popeye.storage.hbase.TsdbFormat._

class TsdbFormatSpec extends FlatSpec with Matchers {

  val samplePoint = {
    val attrNameValues = Seq("name" -> "value", "anotherName" -> "anotherValue")
    val attributes = attrNameValues.map {
      case (name, valueName) => Message.Attribute.newBuilder().setName(name).setValue(valueName).build()
    }.asJava
    Message.Point.newBuilder()
      .setMetric("test")
      .setTimestamp(3610)
      .setIntValue(31)
      .addAllAttributes(attributes)
      .setValueType(Message.Point.ValueType.INT)
      .build()
  }

  val defaultGenerationId: Short = 10
  val defaultGenerationIdBytes = new BytesKey(Bytes.toBytes(defaultGenerationId))

  val defaultShardAttributeName = "name"

  val sampleNamesToIdMapping = Seq(
    (MetricKind, "test") -> bytesKey(1, 0, 1),

    (AttrNameKind, "name") -> bytesKey(2, 0, 1),
    (AttrNameKind, "anotherName") -> bytesKey(2, 0, 2),

    (AttrValueKind, "value") -> bytesKey(3, 0, 1),
    (AttrValueKind, "anotherValue") -> bytesKey(3, 0, 2),

    (ShardKind, shardAttributeToShardName("name", "value")) -> bytesKey(4, 0, 1)
  )

  val sampleIdMap = sampleNamesToIdMapping.map {
    case ((kind, name), id) => qualifiedName(kind, name) -> id
  }.toMap

  behavior of "TsdbFormat.getAllQualifiedNames"

  it should "retrieve qualified names" in {
    val tsdbFormat = createTsdbFormat()
    val allQualifiedNames = sampleIdMap.keys.toSet
    val names: Seq[QualifiedName] = tsdbFormat.getAllQualifiedNames(samplePoint, 0)
    names.toSet should equal(allQualifiedNames)
  }

  behavior of "TsdbFormat.convertToKeyValue"

  it should "create KeyValue rows" in {
    val tsdbFormat = createTsdbFormat()
    val SuccessfulConversion(keyValue) = tsdbFormat.convertToKeyValue(samplePoint, sampleIdMap.get, 0)
    val metricId = Array[Byte](1, 0, 1)
    val valueTypeStructureId = Array[Byte](TsdbFormat.ValueTypes.SingleValueTypeStructureId)
    val downsampling = NoDownsampling
    val downsamplingByte = Array[Byte](TsdbFormat.renderDownsamplingByte(downsampling))
    val timestamp = samplePoint.getTimestamp.toInt
    val timestampBytes = Bytes.toBytes(timestamp - timestamp % downsampling.rowTimespanInSeconds)

    val attr = Array[Byte](2, 0, 1 /*name*/ , 3, 0, 1 /*value*/)
    val anotherAttr = Array[Byte](2, 0, 2 /*name*/ , 3, 0, 2 /*value*/)

    val shardId = Array[Byte](4, 0, 1)

    val sortedAttributeIds = attr ++ anotherAttr

    val row = Seq(
      defaultGenerationIdBytes.bytes,
      downsamplingByte,
      metricId,
      valueTypeStructureId,
      shardId,
      timestampBytes,
      sortedAttributeIds
    ).flatten.toArray
    keyValue.getRow should equal(row)
  }

  it should "convert point values" in {
    val tsdbFormat = createTsdbFormat()
    val SuccessfulConversion(keyValue) = tsdbFormat.convertToKeyValue(samplePoint, sampleIdMap.get, 0)
    val timestamp = samplePoint.getTimestamp.toInt
    val (timeseriesId, baseTime) = parseTimeseriesIdAndBaseTime(CellUtil.cloneRow(keyValue))
    val qualifierBytes = CellUtil.cloneQualifier(keyValue)
    val valueBytes = CellUtil.cloneValue(keyValue)
    val (delta, isFloat) = TsdbFormat.ValueTypes.parseQualifier(qualifierBytes)
    val value = TsdbFormat.ValueTypes.parseSingleValue(valueBytes, isFloat)
    value should equal(Left(samplePoint.getIntValue))
    (baseTime + delta) should equal(timestamp)
  }

  it should "handle int list values" in {
    val tsdbFormat = createTsdbFormat()
    val point = createListPoint("test", 0, Seq(defaultShardAttributeName -> "value"), Left(Seq(1, 2, 3)))
    val SuccessfulConversion(keyValue) = tsdbFormat.convertToKeyValue(point, sampleIdMap.get, 0)
    val timestamp = point.getTimestamp.toInt
    val result = new Result(List(keyValue).asJava)
    parseListValueRowResult(result).lists.head should equal(ListPoint(timestamp, Left(Seq(1, 2, 3))))
  }

  it should "handle float list values" in {
    val tsdbFormat = createTsdbFormat()
    val point = createListPoint("test", 0, Seq(defaultShardAttributeName -> "value"), Right(Seq(1, 2, 3)))
    val SuccessfulConversion(keyValue) = tsdbFormat.convertToKeyValue(point, sampleIdMap.get, 0)
    val timestamp = point.getTimestamp.toInt
    val result = new Result(List(keyValue).asJava)
    parseListValueRowResult(result).lists.head should equal(ListPoint(timestamp, Right(Seq(1, 2, 3))))
  }

  it should "use longer timespans for downsampled timeseries" in {
    val tsdbFormat = createTsdbFormat()
    import DownsamplingResolution._
    val timestamp = timespanInSeconds(Day) - resolutionInSeconds(Day)
    val point = createPoint("test", timestamp, Seq(defaultShardAttributeName -> "value"), Right(1))
    val downsampling = EnabledDownsampling(Day, AggregationType.Max)
    val SuccessfulConversion(keyValue) = tsdbFormat.convertToKeyValue(point, sampleIdMap.get, 0, downsampling)
    val (tsId, baseTime) = TsdbFormat.parseTimeseriesIdAndBaseTime(CellUtil.cloneRow(keyValue))
    baseTime should equal(0)
  }

  ignore should "have good performance" in {
    val tsdbFormat = createTsdbFormat(shardAttributes = (0 until 2).map(i => f"metric_$i").toSet)
    val random = new Random(0)
    val metrics = (0 until 10).map(i => f"metric_$i").toVector
    val attrNames = (0 until 100).map(i => f"name_$i").toVector
    val attrValues = (0 until 1000).map(i => f"value_$i").toVector
    val allNames = metrics.map(metricName => qualifiedName(MetricKind, metricName)) ++
      attrNames.map(attrName => qualifiedName(AttrNameKind, attrName)) ++
      attrValues.map(attrValue => qualifiedName(AttrValueKind, attrValue))
    val allIds = (0 until allNames.size).map(i => Bytes.toBytes(i).slice(1, 4)).map(bytes => new BytesKey(bytes))
    val idMap = (allNames zip allIds).toMap
    def randomElement(elems: IndexedSeq[String]) = elems(random.nextInt(elems.size))
    val points = (0 to 100000).map {
      i =>
        createPoint(
          metric = randomElement(metrics),
          timestamp = random.nextLong(),
          attributes = (0 to random.nextInt(4)).map { _ =>
            (randomElement(attrNames), randomElement(attrValues))
          },
          value = Left(random.nextLong())
        )
    }
    for (_ <- 0 to 100) {
      val startTime = System.currentTimeMillis()
      for (point <- points) {
        tsdbFormat.convertToKeyValue(point, idMap.get, 0) shouldBe a[SuccessfulConversion]
      }
      println(f"time:${ System.currentTimeMillis() - startTime }")
    }
  }

  behavior of "IntListValueType longs serialization"

  it should "serialize empty lists" in {
    val point = createListPoint(value = Left(Seq()))
    val (_, value: Array[Byte]) = IntListValueType.mkQualifiedValue(point, NoDownsampling)
    IntListValueType.parseIntListValue(value).toSeq should be(empty)
  }

  behavior of "FloatListValueType longs serialization"

  it should "serialize empty lists" in {
    val point = createListPoint(value = Right(Seq()))
    val (_, value: Array[Byte]) = FloatListValueType.mkQualifiedValue(point, NoDownsampling)
    FloatListValueType.parseFloatListValue(value).toSeq should be(empty)
  }

  it should "not convert point if not all names are in cache" in {
    val tsdbFormat = createTsdbFormat()
    val notInCache = sampleIdMap.keys.head
    val idCache = (name: QualifiedName) => (sampleIdMap - notInCache).get(name)
    tsdbFormat.convertToKeyValue(samplePoint, idCache, 0) should be(IdCacheMiss)
  }

  behavior of "TsdbFormat.parseRowResult"

  it should "parse row result" in {
    val tsdbFormat = createTsdbFormat()
    val timeAndValues: Seq[(Long, Either[Long, Float])] = Seq(
      (100l, Left(1l)),
      (200l, Right(1.0f)),
      (300l, Left(2l))
    )
    val points = timeAndValues.map {
      case (time, value) =>
        createPoint(
          metric = "test",
          timestamp = time,
          attributes = Seq("name" -> "value", "anotherName" -> "anotherValue"),
          value = value
        )
    }
    val keyValues = points.map {
      point => tsdbFormat.convertToKeyValue(point, sampleIdMap.get, 0).asInstanceOf[SuccessfulConversion].keyValue
    }
    require(keyValues.map(_.getRow.toBuffer).distinct.size == 1)
    val parsedRowResult = parseSingleValueRowResult(new Result(keyValues.asJava))

    val expectedAttributeIds = SortedMap(
      sampleIdMap(qualifiedName(AttrNameKind, "name")) -> sampleIdMap(qualifiedName(AttrValueKind, "value")),
      sampleIdMap(qualifiedName(AttrNameKind, "anotherName")) -> sampleIdMap(qualifiedName(AttrValueKind, "anotherValue"))
    )
    val expectedPoints = timeAndValues.map {
      case (time, value) =>
        Point(time.toInt, value.fold(_.toDouble, _.toDouble))
    }

    val timeseriesId = parsedRowResult.timeseriesId
    timeseriesId.generationId should equal(defaultGenerationIdBytes)
    timeseriesId.metricId should equal(sampleIdMap(qualifiedName(MetricKind, "test")))
    timeseriesId.shardId should equal(bytesKey(4, 0, 1))
    timeseriesId.attributeIds should equal(expectedAttributeIds)
    parsedRowResult.points.iterator.toList should equal(expectedPoints)

  }

  it should "throw meaningful exception if row size is illegal" in {
    val tsdbFormat = createTsdbFormat()
    val row = Array.ofDim[Byte](valueTypeIdOffset + 1)
    row(valueTypeIdOffset) = ValueTypes.SingleValueTypeStructureId
    val keyValue = new KeyValue(row, PointsFamily, Array[Byte](0, 0, 0), Array[Byte](0, 0, 0))
    val ex = intercept[IllegalArgumentException] {
      parseSingleValueRowResult(new Result(Seq(keyValue).asJava))
    }
    ex.getMessage should (include("row") and include("size"))
  }

  def qualifiedName(kind: String, name: String) = QualifiedName(kind, defaultGenerationIdBytes, name)

  def createTsdbFormat(prefixMapping: GenerationIdMapping = new FixedGenerationId(defaultGenerationId),
                       shardAttributes: Set[String] = Set(defaultShardAttributeName)): TsdbFormat = {
    new TsdbFormat(prefixMapping, shardAttributes)
  }

  it should "parse packed row" in {
    val tsdbFormat = createTsdbFormat()
    val timeAndValues: Seq[(Long, Either[Long, Float])] = Seq(
      (100l, Left(1l)),
      (200l, Right(1.0f)),
      (300l, Left(2l))
    )
    val points = timeAndValues.map {
      case (time, value) =>
        createPoint(
          metric = "test",
          timestamp = time,
          attributes = Seq(defaultShardAttributeName -> "value"),
          value = value
        )
    }
    val keyValues = points.map {
      point => tsdbFormat.convertToKeyValue(point, sampleIdMap.get, 0).asInstanceOf[SuccessfulConversion].keyValue
    }
    val packedRow = TsdbFormat.rowPacker.packRow(keyValues)
    val parsedRowResult = TsdbFormat.parseSingleValueRowResult(Result.create(List(packedRow).asJava))
    val expectedPoints = timeAndValues.map {
      case (time, value) =>
        Point(time.toInt, value.fold(_.toDouble, _.toDouble))
    }
    val timeseriesId = parsedRowResult.timeseriesId
    parsedRowResult.points.iterator.toList should equal(expectedPoints)
  }

  it should "parse downsampled timeseries correctly" in {
    val tsdbFormat = createTsdbFormat()
    val point = createPoint("test", 3600 * 24 * 59, Seq(defaultShardAttributeName -> "value"), Right(1))
    val downsampling = EnabledDownsampling(DownsamplingResolution.Day, AggregationType.Max)
    val SuccessfulConversion(keyValue) = tsdbFormat.convertToKeyValue(point, sampleIdMap.get, 0, downsampling)
    val parsedRowResult = TsdbFormat.parseSingleValueRowResult(Result.create(List(keyValue.asInstanceOf[Cell]).asJava))
    val timeseriesId = parsedRowResult.timeseriesId
    parsedRowResult.points.iterator.toList should equal(List(Point(3600 * 24 * 59, 1.0)))
  }

  behavior of "TsdbFormat.getScanNames"

  it should "get qualified names" in {
    val prefixMapping = createGenerationIdMapping((0, MAX_TIMESPAN, 0), (MAX_TIMESPAN, MAX_TIMESPAN * 2, 1))
    val tsdbFormat = createTsdbFormat(prefixMapping, shardAttributes = Set("shard"))
    val attrs = Map(
      "shard" -> SingleValueName("shard_1"),
      "single" -> SingleValueName("name"),
      "mult" -> MultipleValueNames(Seq("mult1", "mult2")),
      "all" -> AllValueNames
    )
    val names = tsdbFormat.getScanNames("test", (0, MAX_TIMESPAN + 1), attrs)

    val expected = Seq(
      (MetricKind, "test"),
      (AttrNameKind, "shard"),
      (AttrNameKind, "single"),
      (AttrNameKind, "mult"),
      (AttrNameKind, "all"),
      (AttrValueKind, "shard_1"),
      (AttrValueKind, "name"),
      (AttrValueKind, "mult1"),
      (AttrValueKind, "mult2"),
      (ShardKind, shardAttributeToShardName("shard", "shard_1"))
    ).flatMap {
      case (kind, name) => Seq(
        QualifiedName(kind, bytesKey(0, 0), name),
        QualifiedName(kind, bytesKey(0, 1), name)
      )
    }.toSet
    if (names != expected) {
      names.diff(expected).foreach(println)
      println("=========")
      expected.diff(names).foreach(println)
    }
    names should equal(expected)
  }

  behavior of "TsdbFormat.getScans"

  it should "create single scan" in {
    val tsdbFormat = createTsdbFormat()
    val scans = tsdbFormat.getScans(
      metric = "test",
      timeRange = (0, 1),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = sampleIdMap,
      TsdbFormat.ValueTypes.SingleValueTypeStructureId,
      NoDownsampling
    )
    scans.size should equal(1)
    val scan = scans(0)
    val downsamplingByte = 0.toByte
    val metricId = Array[Byte](1, 0, 1)
    val valueStructureTypeId = ValueTypes.SingleValueTypeStructureId
    val shardId = Array[Byte](4, 0, 1)
    val startTimestamp = Array[Byte](0, 0, 0, 0)
    val stopTimestamp = Array[Byte](0, 0, 0, 1)
    val rowPrefix =
      defaultGenerationIdBytes.bytes ++
        Array(downsamplingByte) ++
        metricId ++
        Array(valueStructureTypeId) ++
        shardId
    scan.getStartRow should equal(rowPrefix ++ startTimestamp)
    scan.getStopRow should equal(rowPrefix ++ stopTimestamp)
  }

  it should "create 2 scans over generations" in {
    val prefixMapping = createGenerationIdMapping((0, MAX_TIMESPAN, 0), (MAX_TIMESPAN, MAX_TIMESPAN * 2, 1))
    val tsdbFormat = createTsdbFormat(prefixMapping)
    val idMap = sampleNamesToIdMapping.flatMap {
      case ((kind, name), id) => Seq(
        QualifiedName(kind, bytesKey(0, 0), name) -> id,
        QualifiedName(kind, bytesKey(0, 1), name) -> id
      )
    }.toMap
    val scans = tsdbFormat.getScans(
      metric = "test",
      timeRange = (0, MAX_TIMESPAN + 1),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = idMap,
      TsdbFormat.ValueTypes.SingleValueTypeStructureId,
      NoDownsampling
    )
    scans.size should equal(2)
    scans(0).getStartRow.slice(0, uniqueIdGenerationWidth) should equal(Array[Byte](0, 0))
    scans(1).getStartRow.slice(0, uniqueIdGenerationWidth) should equal(Array[Byte](0, 1))
  }

  it should "not create scan if not enough ids resolved" in {
    val prefixMapping = createGenerationIdMapping((0, 3600, 0), (3600, 7200, 1))
    val tsdbFormat = createTsdbFormat(prefixMapping)
    val idMap = sampleNamesToIdMapping.map {
      case ((kind, name), id) => QualifiedName(kind, bytesKey(0, 0), name) -> id
    }.toMap
    val scans = tsdbFormat.getScans(
      metric = "test",
      timeRange = (0, 4000),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = idMap,
      TsdbFormat.ValueTypes.SingleValueTypeStructureId,
      NoDownsampling
    )
    scans.size should equal(1)
    scans(0).getStartRow.slice(0, uniqueIdGenerationWidth) should equal(Array[Byte](0, 0))
  }

  it should "create 2 scan over shards" in {
    val tsdbFormat = createTsdbFormat()
    val idMap = sampleIdMap.updated(
      QualifiedName(ShardKind, defaultGenerationIdBytes, shardAttributeToShardName("name", "anotherValue")),
      bytesKey(4, 0, 2)
    )
    val scans = tsdbFormat.getScans(
      metric = "test",
      timeRange = (0, 4000),
      attributeValueFilters = Map(defaultShardAttributeName -> MultipleValueNames(Seq("value", "anotherValue"))),
      idMap = idMap,
      TsdbFormat.ValueTypes.SingleValueTypeStructureId,
      NoDownsampling
    )
    scans.size should equal(2)
    scans(0).getStartRow.slice(shardIdOffset, shardIdOffset + shardIdWidth) should equal(Array[Byte](4, 0, 1))
    scans(1).getStartRow.slice(shardIdOffset, shardIdOffset + shardIdWidth) should equal(Array[Byte](4, 0, 2))
  }

  it should "include value_type_structure_id byte flag" in {
    val tsdbFormat = createTsdbFormat()
    val scans = tsdbFormat.getScans(
      metric = "test",
      timeRange = (0, 1),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = sampleIdMap,
      TsdbFormat.ValueTypes.ListValueTypeStructureId,
      NoDownsampling
    )
    scans.size should equal(1)
    scans(0).getStartRow()(valueTypeIdOffset) should equal(ValueTypes.ListValueTypeStructureId)
  }

  it should "include downsampling byte flag" in {
    val tsdbFormat = createTsdbFormat()
    val scans = tsdbFormat.getScans(
      metric = "test",
      timeRange = (0, 1),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = sampleIdMap,
      TsdbFormat.ValueTypes.ListValueTypeStructureId,
      EnabledDownsampling(DownsamplingResolution.Minute5, AggregationType.Min)
    )
    scans.size should equal(1)
    scans(0).getStartRow()(downsamplingQualByteOffset) should equal(0x12.toByte)
  }

  it should "use correct timespan (downsampling case)" in {
    import DownsamplingResolution._
    val timestamp = timespanInSeconds(Day) - resolutionInSeconds(Day)
    val tsdbFormat = createTsdbFormat()
    val scans = tsdbFormat.getScans(
      metric = "test",
      timeRange = (0, 1),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = sampleIdMap,
      TsdbFormat.ValueTypes.ListValueTypeStructureId,
      EnabledDownsampling(Day, AggregationType.Min)
    )
    scans.size should equal(1)
    val row = scans(0).getStartRow()
    Bytes.toInt(row, baseTimeOffset) should equal(0)
  }

  behavior of "TsdbFormat.createRowRegexp"

  it should "handle a simple case" in {
    val attributes = Map(bytesKey(0, 0, 1) -> SingleValueId(bytesKey(0, 0, 1)))
    val regexp = TsdbFormat.createRowRegexp(offset = 7, attrNameLength = 3, attrValueLength = 3, attributes)
    val pattern = Pattern.compile(regexp)

    val validRow = bytesToString(bytesKey(0, 0, 2, 76, -45, -71, -128, 0, 0, 1, 0, 0, 1))
    pattern.matcher(validRow).matches() should be(true)

    val invalidRow = bytesToString(bytesKey(0, 0, 2, 76, -45, -71, -128, 0, 0, 1, 0, 0, 3))
    pattern.matcher(invalidRow).matches() should be(false)
  }

  it should "check attribute name length" in {
    val attributes = Map(bytesKey(0) -> SingleValueId(bytesKey(0)))
    val exception = intercept[IllegalArgumentException] {
      TsdbFormat.createRowRegexp(offset = 7, attrNameLength = 3, attrValueLength = 1, attributes)
    }
    exception.getMessage should (include("3") and include("1") and include("name"))
  }

  it should "check attribute value length" in {
    val attributes = Map(bytesKey(0) -> SingleValueId(bytesKey(0)))
    val exception = intercept[IllegalArgumentException] {
      TsdbFormat.createRowRegexp(offset = 7, attrNameLength = 1, attrValueLength = 3, attributes)
    }
    exception.getMessage should (include("3") and include("1") and include("value"))
  }

  it should "check that attribute name and value length is greater than zero" in {
    intercept[IllegalArgumentException] {
      TsdbFormat.createRowRegexp(
        offset = 7,
        attrNameLength = 0,
        attrValueLength = 1,
        attributes = Map(bytesKey(0) -> SingleValueId(bytesKey(0)))
      )
    }.getMessage should (include("0") and include("name"))
    intercept[IllegalArgumentException] {
      TsdbFormat.createRowRegexp(
        offset = 7,
        attrNameLength = 1,
        attrValueLength = 0,
        attributes = Map(bytesKey(0) -> SingleValueId(bytesKey(0)))
      )
    }.getMessage should (include("0") and include("value"))
  }

  it should "check that attribute list is not empty" in {
    val exception = intercept[IllegalArgumentException] {
      TsdbFormat.createRowRegexp(offset = 7, attrNameLength = 1, attrValueLength = 2, attributes = Map())
    }
    exception.getMessage should include("empty")
  }

  it should "escape regex escaping sequences symbols" in {
    val badStringBytes = stringToBytes("aaa\\Ebbb")
    val attrName = badStringBytes
    val attrValue = badStringBytes
    val rowRegexp = TsdbFormat.createRowRegexp(offset = 0, attrName.length, attrValue.length, Map(attrName -> SingleValueId(attrValue)))
    val rowString = createRowString(attrs = List((attrName, attrValue)))
    rowString should fullyMatch regex rowRegexp
  }

  it should "escape regex escaping sequences symbols (non-trivial case)" in {
    val attrName = stringToBytes("aaa\\")
    val attrValue = stringToBytes("Eaaa")
    val regexp = TsdbFormat.createRowRegexp(offset = 0, attrName.length, attrValue.length, Map(attrName -> SingleValueId(attrValue)))
    val rowString = createRowString(attrs = List((attrName, attrValue)))
    rowString should fullyMatch regex regexp
  }

  it should "handle newline characters" in {
    val attrName = stringToBytes("attrName")
    val attrValue = stringToBytes("attrValue")
    val rowRegexp = TsdbFormat.createRowRegexp(offset = 1, attrName.length, attrValue.length, Map(attrName -> SingleValueId(attrValue)))
    val row = createRow(prefix = stringToBytes("\n"), List((attrName, attrValue)))
    val rowString = bytesToString(row)
    rowString should fullyMatch regex rowRegexp
  }

  it should "create regexp for multiple value filtering" in {
    val attrName = stringToBytes("attrName")
    val attrValues = List(bytesKey(1), bytesKey(2))
    val rowRegexp = TsdbFormat.createRowRegexp(
      offset = 0,
      attrName.length,
      attrValueLength = 1,
      Map((attrName, MultipleValueIds(attrValues)))
    )

    createRowString(attrs = List((attrName, bytesKey(1)))) should fullyMatch regex rowRegexp
    createRowString(attrs = List((attrName, bytesKey(2)))) should fullyMatch regex rowRegexp
    createRowString(attrs = List((attrName, bytesKey(3)))) should not(fullyMatch regex rowRegexp)
  }

  it should "create regexp for any value filtering" in {
    val attrName = stringToBytes("attrName")
    val rowRegexp = TsdbFormat.createRowRegexp(
      offset = 0,
      attrName.length,
      attrValueLength = 1,
      Map(attrName -> AllValueIds)
    )

    createRowString(attrs = List((attrName, bytesKey(1)))) should fullyMatch regex rowRegexp
    createRowString(attrs = List((attrName, bytesKey(100)))) should fullyMatch regex rowRegexp
    createRowString(attrs = List((stringToBytes("ATTRNAME"), bytesKey(1)))) should not(fullyMatch regex rowRegexp)
  }

  it should "pass randomized test" in {
    implicit val random = deterministicRandom
    for (_ <- 0 to 100) {
      val offset = random.nextInt(10)
      val attrNameLength = random.nextInt(5) + 1
      val attrValueLength = random.nextInt(5) + 1
      val searchAttrs = randomAttributes(attrNameLength, attrValueLength)
      val attrsForRegexp = searchAttrs.map { case (n, v) => (n, SingleValueId(v)) }.toMap
      val searchAttrNamesSet = searchAttrs.map { case (name, _) => name.bytes.toList }.toSet
      val rowRegexp = TsdbFormat.createRowRegexp(offset, attrNameLength, attrValueLength, attrsForRegexp)
      def createJunkAttrs() = randomAttributes(attrNameLength, attrValueLength).filter {
        case (name, _) => !searchAttrNamesSet(name.bytes.toList)
      }
      for (_ <- 0 to 10) {
        val junkAttrs = createJunkAttrs()
        val rowString = arrayToString(createRow(offset, searchAttrs ++ junkAttrs))
        if (!Pattern.matches(rowRegexp, rowString)) {
          println(attrNameLength)
          println(attrValueLength)
          println(stringToBytes(rowRegexp).bytes.toList)
          println(stringToBytes(rowString).bytes.toList)
        }
        rowString should fullyMatch regex rowRegexp

        val anotherJunkAttrs = createJunkAttrs()
        val anotherRowString = arrayToString(createRow(offset, anotherJunkAttrs ++ junkAttrs))
        anotherRowString should not(fullyMatch regex rowRegexp)
      }
    }
  }

  behavior of "TsdbFormat downsampling descriptor"

  import DownsamplingResolution._
  import AggregationType._

  it should "render zero byte" in {
    TsdbFormat.renderDownsamplingByte(NoDownsampling) should equal(0.toByte)
    TsdbFormat.parseDownsamplingByte(0) should equal(NoDownsampling)
  }

  it should "render byte for m5:min" in {
    val downsampling = EnabledDownsampling(Minute5, Min)
    val dsByte = TsdbFormat.renderDownsamplingByte(downsampling)
    dsByte should equal(0x12.toByte)
  }

  it should "handle roundtrips" in {
    def roundtrip(downsampling: Downsampling) = {
      val dsByte = TsdbFormat.renderDownsamplingByte(downsampling)
      val parsedDs = TsdbFormat.parseDownsamplingByte(dsByte)
      parsedDs should equal(downsampling)
    }
    roundtrip(EnabledDownsampling(Minute5, Max))
    roundtrip(EnabledDownsampling(Hour, Avg))
    roundtrip(EnabledDownsampling(Day, Min))
  }

  def deterministicRandom: Random = {
    new Random(0)
  }

  def randomBytes(nBytes: Int)(implicit random: Random): List[Byte] = {
    val array = new Array[Byte](nBytes)
    random.nextBytes(array)
    array.toList
  }

  def randomAttributes(attrNameLength: Int, attrValueLength: Int)(implicit random: Random) = {
    val randomAttrs =
      for (_ <- 0 to random.nextInt(7))
      yield {
        (randomBytes(attrNameLength), randomBytes(attrValueLength))
      }
    // uniquify attribute names
    randomAttrs.toMap.toList.map {
      case (attrName, attrValue) => (new BytesKey(attrName.toArray), new BytesKey(attrValue.toArray))
    }
  }

  def createRow(prefix: Array[Byte], attrs: List[(BytesKey, BytesKey)]) = {
    val sorted = attrs.sortBy(_._1)
    prefix ++ sorted.map(pair => pair._1.bytes ++ pair._2.bytes).foldLeft(Array[Byte]())(_ ++ _)
  }

  def createRow(offset: Int, attrs: List[(BytesKey, BytesKey)])(implicit random: Random): Array[Byte] =
    createRow(randomBytes(offset).toArray, attrs)

  def createRowString(prefix: Array[Byte] = Array.empty[Byte], attrs: List[(BytesKey, BytesKey)]) =
    bytesToString(createRow(prefix, attrs))

  private def bytesToString(bKey: BytesKey) = arrayToString(bKey.bytes)

  private def arrayToString(array: Array[Byte]) = new String(array, TsdbFormat.ROW_REGEX_FILTER_ENCODING)

  private def stringToBytes(string: String): BytesKey = {
    val charBuffer = CharBuffer.wrap(string)
    new BytesKey(TsdbFormat.ROW_REGEX_FILTER_ENCODING.encode(charBuffer).array())
  }

}
