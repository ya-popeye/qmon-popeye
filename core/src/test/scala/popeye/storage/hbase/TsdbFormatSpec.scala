package popeye.storage.hbase

import org.scalatest.{Matchers, FlatSpec}
import popeye.proto.Message
import scala.collection.JavaConverters._
import popeye.storage.hbase.HBaseStorage.{QualifiedName, MetricKind, AttrNameKind, AttrValueKind}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result

class TsdbFormatSpec extends FlatSpec with Matchers {
  behavior of "TsdbFormat"

  val tsdbFormat = new TsdbFormat

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
  val sampleIdMap = Map(
    QualifiedName(MetricKind, samplePoint.getMetric) -> Array(0, 0, 1),
    QualifiedName(AttrNameKind, "name") -> Array(0, 0, 1),
    QualifiedName(AttrNameKind, "anotherName") -> Array(0, 0, 2),
    QualifiedName(AttrValueKind, "value") -> Array(0, 0, 1)
  ).mapValues(array => new BytesKey(array.map(_.toByte)))


  it should "retrieve qualified names" in {
    val allQualifiedNames = sampleIdMap.keys.toSet
    val names: List[QualifiedName] = TsdbFormat.getAllQualifiedNames(samplePoint)
    names.toSet should equal(allQualifiedNames)
    names.distinct should equal(names)
  }

  it should "convert point to KeyValue" in {
    val keyValue = tsdbFormat.convertToKeyValue(samplePoint, sampleIdMap)
    val metricId = Array(0, 0, 1).map(_.toByte)
    val timestamp = samplePoint.getTimestamp.toInt
    val timestampBytes = Bytes.toBytes(timestamp - timestamp % HBaseStorage.MAX_TIMESPAN)
    val sortedAttributeIds =
      (Array(0, 0, 1) ++ Array(0, 0, 1) ++ // first attr
        Array(0, 0, 2) ++ Array(0, 0, 1)).map(_.toByte) // second attr
    val row = metricId ++ timestampBytes ++ sortedAttributeIds
    keyValue.getRow should equal(row)
    val result = new Result(List(keyValue).asJava)
    tsdbFormat.parsePoints(result).head should equal(HBaseStorage.Point(timestamp, samplePoint.getIntValue))
  }

  it should "convert point if all names are in cache" in {
    val (partiallyConvertedPoints, keyValues) = tsdbFormat.convertToKeyValues(Seq(samplePoint), sampleIdMap.get)
    partiallyConvertedPoints.points.isEmpty should be(true)
    keyValues.size should equal(1)
    keyValues.head should equal(tsdbFormat.convertToKeyValue(samplePoint, sampleIdMap))
    val point = HBaseStorage.Point(samplePoint.getTimestamp.toInt, samplePoint.getIntValue)
    tsdbFormat.parsePoints(new Result(keyValues.asJava)).head should equal(point)
  }

  it should "not convert point if not all names are in cache" in {
    val notInCache = QualifiedName(AttrNameKind, "name")
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

}
