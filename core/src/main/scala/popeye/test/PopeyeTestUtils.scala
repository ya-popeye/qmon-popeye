package popeye.test

import popeye.proto.Message.Point
import java.util.concurrent.atomic.AtomicLong
import java.util.Random
import java.text.SimpleDateFormat
import popeye.proto.Message
import scala.collection.JavaConversions.iterableAsScalaIterable
import popeye.storage.hbase.{TimeRangeAndId, GenerationIdMapping, BytesKey}

import scala.collection.JavaConverters._

/**
 * @author Andrey Stepachev
 */
object PopeyeTestUtils {
  val ts = new AtomicLong(new SimpleDateFormat("yyyy/MM/dd").parse("2011/11/11").getTime/1000)

  def names: List[String] = List("my.metric1", "proc.net.bytes", "proc.fs.descriptors")

  def hosts: List[String] = List("test.yandex.ru", "localhost", "other.com")

  def telnetCommand(point: Message.Point) = {
    s"put ${point.getMetric} ${point.getTimestamp} ${point.getIntValue} " +
      point.getAttributesList.map { attr =>
        attr.getName + "=" + attr.getValue
      }.mkString(" ")
  }

  def makeBatch(msgs: Int = 2,
                names: List[String] = names,
                hosts: List[String] = hosts)
               (implicit rnd: Random): Seq[Point] = {
    mkEvents(msgs, names, hosts).toSeq
  }

  def mkEvents(msgs: Int = 2,
               names: List[String] = names,
               hosts: List[String] = hosts)
              (implicit rnd: Random): Seq[Point] = {
    0 until msgs collect {
      case i => mkEvent(names, hosts)
    }
  }

  def mkEvent(names: List[String] = names,
              hosts: List[String] = hosts)
             (implicit rnd: Random): Point = {
    val host: String = hosts(rnd.nextInt(hosts.length))
    val timestamp: Long = ts.addAndGet(rnd.nextInt(2000) + 1000l)
    val name: String = names(rnd.nextInt(names.length))
    createPoint(
      metric = name,
      timestamp = timestamp,
      attributes = Seq("host" -> host),
      value = Left(rnd.nextLong())
    )
  }

  def bytesKey(bytes: Byte*) = new BytesKey(Array[Byte](bytes: _*))

  def createTimeRangeIdMapping(ranges: (Int, Int, Short)*) = new GenerationIdMapping {
    val sortedRanges = ranges.sortBy(r => -r._1)

    override def backwardIterator(timestampInSeconds: Int): Iterator[TimeRangeAndId] = {
      sortedRanges.iterator
        .map { case (start, stop, id) => TimeRangeAndId(start, stop, id) }
        .dropWhile(range => !range.contains(timestampInSeconds))
    }

    override def getGenerationId(timestampInSeconds: Int, currentTimeInSeconds: Int): Short =
      backwardIterator(timestampInSeconds).next().id
  }

  def createPoint(metric: String = "metric",
                  timestamp: Long = 0,
                  attributes: Seq[(String, String)] = Seq("host" -> "localhost"),
                  value: Either[Long, Float] = Left(0)) = {
    val builder = pointBuilder(metric, timestamp, attributes)
    value.fold(
      longVal => {
        builder.setValueType(Message.Point.ValueType.INT)
        builder.setIntValue(longVal)
      },
      floatVal => {
        builder.setValueType(Message.Point.ValueType.FLOAT)
        builder.setFloatValue(floatVal)
      }
    )
    builder.build()
  }

  def createListPoint(metric: String = "metric",
                      timestamp: Long = 0,
                      attributes: Seq[(String, String)] = Seq("host" -> "localhost"),
                      value: Either[Seq[Long], Seq[Float]] = Left(Seq())) = {
    import scala.collection.JavaConverters._
    val builder = pointBuilder(metric, timestamp, attributes)
    value.fold(
      longsVal => {
        builder.setValueType(Message.Point.ValueType.INT_LIST)
        builder.addAllIntListValue(longsVal.map(l => java.lang.Long.valueOf(l)).asJava)
      },
      floatsVal => {
        builder.setValueType(Message.Point.ValueType.FLOAT_LIST)
        builder.addAllFloatListValue(floatsVal.map(f => java.lang.Float.valueOf(f)).asJava)
      }
    )
    builder.build()
  }

  private def pointBuilder(metric: String, timestamp: Long, attributes: Seq[(String, String)]) = {
    val attrs = attributes.map {
      case (aName, aValue) => Message.Attribute.newBuilder().setName(aName).setValue(aValue).build()
    }.asJava
    val builder = Point.newBuilder()
      .setMetric(metric)
      .setTimestamp(timestamp)
      .addAllAttributes(attrs)
    builder
  }

  def time[T](body: => Unit) = {
    val startTime = System.currentTimeMillis()
    body
    System.currentTimeMillis() - startTime
  }
}
