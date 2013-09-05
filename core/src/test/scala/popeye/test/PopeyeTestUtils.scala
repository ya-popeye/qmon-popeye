package popeye.test

import popeye.transport.proto.Message.{Attribute, Point}
import java.util.concurrent.atomic.AtomicLong
import java.util.Random
import java.text.SimpleDateFormat

/**
 * @author Andrey Stepachev
 */
object PopeyeTestUtils {
  val ts = new AtomicLong(new SimpleDateFormat("yyyy/MM/dd").parse("2011/11/11").getTime/1000)

  def names: List[String] = List("my.metric1", "proc.net.bytes", "proc.fs.descriptors")

  def hosts: List[String] = List("test.yandex.ru", "localhost", "other.com")

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


}
