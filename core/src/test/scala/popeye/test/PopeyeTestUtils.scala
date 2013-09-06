package popeye.test

import popeye.transport.proto.Message.{Attribute, Point}
import java.util.concurrent.atomic.AtomicLong
import java.util.Random
import java.text.SimpleDateFormat
import popeye.transport.proto.Message
import scala.collection.JavaConversions.iterableAsScalaIterable
import popeye.transport.kafka.{PopeyeKafkaConsumer, PopeyeKafkaConsumerFactory}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

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
    Point.newBuilder()
      .setTimestamp(timestamp)
      .setIntValue(rnd.nextLong())
      .setMetric(name)
      .addAttributes(Attribute.newBuilder()
      .setName("host")
      .setValue(host)
    ).build()
  }

  class MockPopeyeConsumerFacotory extends PopeyeKafkaConsumerFactory {

    val consumer = new MockPopeyeConsumer

    def newConsumer(): PopeyeKafkaConsumer = consumer
  }

  class MockPopeyeConsumer extends PopeyeKafkaConsumer {

    var list = List[Option[(Long, Seq[Point])]]()
    var isCommit = false
    var isShutdown = false

    def addMessages(batchId: Long, points: Seq[Point]) = {
      list = list :+ Some(batchId -> points)
    }

    def iterateTopic(topic: String): Iterator[Option[(Long, Seq[Point])]] = {
      list.iterator
    }

    def commitOffsets() {
      isCommit = true
    }

    def shutdown() {
      isShutdown = true
    }
  }

  class MockAnswer[T](function: Any => T) extends Answer[T] {
    def answer(invocation: InvocationOnMock): T = {
      val args = invocation.getArguments
      val mock = invocation.getMock
      if (args.size == 0) {
        function match {
          case f: Function0[_] => return f()
          case f: Function1[_,_] => return f(mock)
        }
      } else if (args.size == 1) {
        function match {
          case f: Function1[_, _] => return f(args(0))
        }
        function match {
          case f2: Function2[_, _, _] => return f2(args(0), mock)
        }
      } else {
        function match {
          case f: Function1[_, _] => return f(args)
        }
        function match {
          case f2: Function2[_, _, _] => return f2(args, mock)
        }
      }
    }
  }
}
