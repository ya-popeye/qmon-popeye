package popeye.javaapi.storage.hbase

import popeye.storage.hbase.{TsdbFormat => ScalaTsdbFormat, PartiallyConvertedPoints => ScalaPartiallyConvertedPoints, BytesKey}
import popeye.proto.Message
import scala.collection.JavaConverters._
import popeye.storage.hbase.HBaseStorage.QualifiedName
import org.apache.hadoop.hbase.KeyValue


class PartiallyConvertedPoints(points: ScalaPartiallyConvertedPoints) {
  def unresolvedNames() = {
    points.unresolvedNames.asJava
  }

  def convert(partialIdMap: java.util.Map[QualifiedName, BytesKey]): java.util.List[KeyValue] = {
    points.convert(partialIdMap.asScala.toMap).asJava
  }
}

case class PartiallyConvertedPointsAndKeyValues(partialPoints: PartiallyConvertedPoints, keyValues: java.util.List[KeyValue])

class TsdbFormat {
  val scalaFormat = new ScalaTsdbFormat()

  def convertToKeyValues(points: java.lang.Iterable[Message.Point],
                         idCache: java.util.Map[QualifiedName, BytesKey]): PartiallyConvertedPointsAndKeyValues = {
    val (partial, keyValues) = scalaFormat.convertToKeyValues(points.asScala, name => Option(idCache.get(name)))
    PartiallyConvertedPointsAndKeyValues(new PartiallyConvertedPoints(partial), keyValues.asJava)
  }

  def convertToKeyValue(point: Message.Point, idMap: java.util.Map[QualifiedName, BytesKey]) = {
    scalaFormat.convertToKeyValue(point, idMap.asScala.toMap)
  }

  def getAllQualifiedNames(point: Message.Point): java.util.List[QualifiedName] = {
    ScalaTsdbFormat.getAllQualifiedNames(point).asJava
  }
}
