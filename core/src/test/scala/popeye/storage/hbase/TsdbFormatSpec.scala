package popeye.storage.hbase

import org.scalatest.{Matchers, FlatSpec}
import popeye.proto.Message
import scala.collection.JavaConverters._
import popeye.storage.hbase.HBaseStorage.{MetricKind, AttrNameKind, AttrValueKind}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import popeye.storage.hbase.HBaseStorage.QualifiedName

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

  val sampleIdMap = Map(
    qualifiedName(MetricKind, samplePoint.getMetric) -> Array(0, 0, 1),
    qualifiedName(AttrNameKind, "name") -> Array(0, 0, 1),
    qualifiedName(AttrNameKind, "anotherName") -> Array(0, 0, 2),
    qualifiedName(AttrValueKind, "value") -> Array(0, 0, 1)
  ).mapValues(array => new BytesKey(array.map(_.toByte)))

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
    val metricId = Array(0, 0, 1).map(_.toByte)
    val timestamp = samplePoint.getTimestamp.toInt
    val timestampBytes = Bytes.toBytes(timestamp - timestamp % HBaseStorage.MAX_TIMESPAN)
    val sortedAttributeIds =
      (Array(0, 0, 1) ++ Array(0, 0, 1) ++ // first attr
        Array(0, 0, 2) ++ Array(0, 0, 1)).map(_.toByte) // second attr
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

  def createTsdbFormat(timeRangeIdMapping: Long => Array[Byte] = _ => defaultNamespace): TsdbFormat = {
    new TsdbFormat(new TimeRangeIdMapping {
      override def getRangeId(timestampInSeconds: Long): BytesKey =
        new BytesKey(timeRangeIdMapping(timestampInSeconds))
    })
  }
}
