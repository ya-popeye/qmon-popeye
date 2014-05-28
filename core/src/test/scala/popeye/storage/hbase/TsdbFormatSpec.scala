package popeye.storage.hbase

import org.scalatest.{Matchers, FlatSpec}
import popeye.proto.Message
import scala.collection.JavaConverters._
import popeye.storage.hbase.HBaseStorage._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import popeye.test.PopeyeTestUtils._
import scala.collection.immutable.SortedMap
import org.apache.hadoop.hbase.KeyValue
import popeye.storage.hbase.HBaseStorage.ValueIdFilterCondition._
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition._
import java.nio.CharBuffer
import java.util.regex.Pattern
import scala.util.Random
import popeye.storage.hbase.HBaseStorage.ValueIdFilterCondition.SingleValueId
import popeye.storage.hbase.HBaseStorage.ValueIdFilterCondition.MultipleValueIds
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition.MultipleValueNames
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition.SingleValueName

class TsdbFormatSpec extends FlatSpec with Matchers {
  behavior of "TsdbFormat"

  val samplePoint = {
    val attrNameValues = Seq("name" -> "value", "anotherName" -> "value")
    val attributes = attrNameValues.map {
      case (name, valueName) => Message.Attribute.newBuilder().setName(name).setValue(valueName).build()
    }.asJava
    Message.Point.newBuilder()
      .setMetric("test")
      .setTimestamp(3610)
      .setIntValue(31)
      .addAllAttributes(attributes)
      .build()
  }

  val defaultNamespace = new BytesKey(Array[Byte](10, 10))

  val sampleNamesToIdMapping = Seq(
    (MetricKind, "test") -> bytesKey(0, 0, 1),
    (AttrNameKind, "name") -> bytesKey(0, 0, 2),
    (AttrNameKind, "anotherName") -> bytesKey(0, 0, 3),
    (AttrValueKind, "value") -> bytesKey(0, 0, 4)
  )

  val sampleIdMap = sampleNamesToIdMapping.map {
    case ((kind, name), id) => qualifiedName(kind, name) -> id
  }.toMap

  def qualifiedName(kind: String, name: String) = QualifiedName(kind, defaultNamespace, name)

  it should "retrieve qualified names" in {
    val tsdbFormat = createTsdbFormat()
    val allQualifiedNames = sampleIdMap.keys.toSet
    val names: Seq[QualifiedName] = tsdbFormat.getAllQualifiedNames(samplePoint)
    names.toSet should equal(allQualifiedNames)
    names.distinct should equal(names)
  }

  it should "create KeyValue rows" in {
    val tsdbFormat = createTsdbFormat()
    val keyValue = tsdbFormat.convertToKeyValue(samplePoint, sampleIdMap)
    val metricId = Array[Byte](0, 0, 1)
    val timestamp = samplePoint.getTimestamp.toInt
    val timestampBytes = Bytes.toBytes(timestamp - timestamp % TsdbFormat.MAX_TIMESPAN)
    val sortedAttributeIds =
      (Array(0, 0, 2) ++ Array(0, 0, 4) ++ // first attr
        Array(0, 0, 3) ++ Array(0, 0, 4)).map(_.toByte) // second attr
    val row = defaultNamespace.bytes ++ metricId ++ timestampBytes ++ sortedAttributeIds
    keyValue.getRow should equal(row)
  }

  it should "convert point values" in {
    val tsdbFormat = createTsdbFormat()
    val keyValue = tsdbFormat.convertToKeyValue(samplePoint, sampleIdMap)
    val timestamp = samplePoint.getTimestamp.toInt
    val result = new Result(List(keyValue).asJava)
    tsdbFormat.parsePoints(result).head should equal(HBaseStorage.Point(timestamp, samplePoint.getIntValue))
  }

  it should "convert point if all names are in cache" in {
    val tsdbFormat = createTsdbFormat()
    val (partiallyConvertedPoints, keyValues) = tsdbFormat.convertToKeyValues(Seq(samplePoint), sampleIdMap.get)
    partiallyConvertedPoints.points.isEmpty should be(true)
    keyValues.size should equal(1)
    keyValues.head should equal(tsdbFormat.convertToKeyValue(samplePoint, sampleIdMap))
    val point = HBaseStorage.Point(samplePoint.getTimestamp.toInt, samplePoint.getIntValue)
    tsdbFormat.parsePoints(new Result(keyValues.asJava)).head should equal(point)
  }

  it should "not convert point if not all names are in cache" in {
    val tsdbFormat = createTsdbFormat()
    val notInCache = sampleIdMap.keys.head
    val idCache = (name: QualifiedName) => (sampleIdMap - notInCache).get(name)
    val (partiallyConvertedPoints, keyValues) = tsdbFormat.convertToKeyValues(Seq(samplePoint), idCache)
    partiallyConvertedPoints.points.isEmpty should be(false)
    keyValues.isEmpty should be(true)
    partiallyConvertedPoints.unresolvedNames should equal(Set(notInCache))
    val delayedkeyValues = partiallyConvertedPoints.convert(Map(notInCache -> sampleIdMap(notInCache)))
    delayedkeyValues.head should equal(tsdbFormat.convertToKeyValue(samplePoint, sampleIdMap))
    val point = HBaseStorage.Point(samplePoint.getTimestamp.toInt, samplePoint.getIntValue)
    tsdbFormat.parsePoints(new Result(delayedkeyValues.asJava)).head should equal(point)
  }

  it should "parse row result" in {
    val tsdbFormat = createTsdbFormat()
    val timeAndValues: Seq[(Long, Either[Long, Float])] = Seq(
      (100, Left(1l)),
      (200, Right(1.0f)),
      (300, Left(2l))
    )
    val points = timeAndValues.map {
      case (time, value) =>
        createPoint(
          metric = "test",
          timestamp = time,
          attributes = Seq("name" -> "value"),
          value = value
        )
    }
    val keyValues = points.map {
      point => tsdbFormat.convertToKeyValue(point, sampleIdMap)
    }
    require(keyValues.map(_.getRow.toBuffer).distinct.size == 1)
    val parsedRowResult = tsdbFormat.parseRowResult(new Result(keyValues.asJava))

    val expected = ParsedRowResult(
      namespace = defaultNamespace,
      metricId = sampleIdMap(qualifiedName(MetricKind, "test")),
      attributeIds = SortedMap(
        sampleIdMap(qualifiedName(AttrNameKind, "name")) -> sampleIdMap(qualifiedName(AttrValueKind, "value"))
      ),
      points = timeAndValues.map { case (time, value) => HBaseStorage.Point(time.toInt, value) }
    )
    parsedRowResult should equal(expected)
  }

  it should "throw meaningful exception if row size is illegal" in {
    val tsdbFormat = createTsdbFormat()
    val row = Array[Byte](0)
    val keyValue = new KeyValue(row, HBaseStorage.PointsFamily, Array[Byte](0, 0, 0), Array[Byte](0, 0, 0))
    val ex = intercept[IllegalArgumentException] {
      tsdbFormat.parseRowResult(new Result(Seq(keyValue).asJava))
    }
    ex.getMessage should (include("row") and include("size"))
  }

  def createTsdbFormat(timeRangeIdMapping: Int => Seq[Int] = _ => defaultNamespace.bytes.map(_.toInt)): TsdbFormat = {
    new TsdbFormat(new TimeRangeIdMapping {
      override def getRangeId(timestampInSeconds: Int): BytesKey =
        new BytesKey(timeRangeIdMapping(timestampInSeconds).map(_.toByte).toArray)
    })
  }

  behavior of "TsdbFormat.getScanNames"

  it should "get qualified names" in {
    val tsdbFormat = createTsdbFormat(time => Seq(0, time / 3600))
    val attrs = Map(
      "single" -> SingleValueName("name"),
      "mult" -> MultipleValueNames(Seq("mult1", "mult2")),
      "all" -> AllValueNames
    )
    val names = tsdbFormat.getScanNames("test", (0, 4000), attrs)

    val expected = Seq(
      (MetricKind, "test"),
      (AttrNameKind, "single"),
      (AttrNameKind, "mult"),
      (AttrNameKind, "all"),
      (AttrValueKind, "name"),
      (AttrValueKind, "mult1"),
      (AttrValueKind, "mult2")
    ).flatMap {
      case (kind, name) => Seq(
        QualifiedName(kind, bytesKey(0, 0), name),
        QualifiedName(kind, bytesKey(0, 1), name)
      )
    }.toSet
    names should equal(expected)
  }

  it should "choose namespace by base time" in {
    val tsdbFormat = createTsdbFormat {
      timestamp =>
        timestamp should equal(3600)
        Seq(0, 1)
    }
    tsdbFormat.getScanNames("", (4000, 4001), Map())
  }

  behavior of "TsdbFormat.getScans"

  it should "create single scan" in {
    val tsdbFormat = createTsdbFormat()
    val scans = tsdbFormat.getScans("test", (0, 1), Map(), sampleIdMap)
    scans.size should equal(1)
    val scan = scans(0)
    scan.getStartRow should equal(defaultNamespace.bytes ++ Array[Byte](0, 0, 1, /**/ 0, 0, 0, 0))
    scan.getStopRow should equal(defaultNamespace.bytes ++ Array[Byte](0, 0, 1, /**/ 0, 0, 0, 1))
  }

  it should "create 2 scans" in {
    val tsdbFormat = createTsdbFormat(time => Seq(0, time / 3600))
    val idMap = sampleNamesToIdMapping.flatMap {
      case ((kind, name), id) => Seq(
        QualifiedName(kind, bytesKey(0, 0), name) -> id,
        QualifiedName(kind, bytesKey(0, 1), name) -> id
      )
    }.toMap
    val scans = tsdbFormat.getScans("test", (0, 4000), Map(), idMap)
    scans.size should equal(2)
    scans(0).getStartRow.slice(0, UniqueIdNamespaceWidth) should equal(Array[Byte](0, 0))
    scans(1).getStartRow.slice(0, UniqueIdNamespaceWidth) should equal(Array[Byte](0, 1))
  }

  it should "not create scan if not enough ids resolved" in {
    val tsdbFormat = createTsdbFormat(time => Seq(0, time / 3600))
    val idMap = sampleNamesToIdMapping.flatMap {
      case ((kind, name), id) => Seq(
        QualifiedName(kind, bytesKey(0, 0), name) -> id
      )
    }.toMap
    val scans = tsdbFormat.getScans("test", (0, 4000), Map(), idMap)
    scans.size should equal(1)
    scans(0).getStartRow.slice(0, UniqueIdNamespaceWidth) should equal(Array[Byte](0, 0))
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
