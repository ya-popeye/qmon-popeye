package popeye.test

import popeye.transport.proto.Message.{Attribute, Point}
import java.util.concurrent.atomic.AtomicLong
import java.util.Random

/**
 * @author Andrey Stepachev
 */
object PopeyeTestUtils {
  val ts = new AtomicLong(1234123412)

  def tsStream = Stream.continually(ts.incrementAndGet())

  def makeBatch(msgs: Int = 2, timestamps: Stream[Long] = tsStream)(implicit rnd: Random): Seq[Point] = {
    mkEvents(msgs, timestamps).toSeq
  }

  def mkEvents(msgs: Int = 2, timestamps: Stream[Long] = tsStream)(implicit rnd: Random): Seq[Point] = {
    0 until msgs collect {
      case i => mkEvent(timestamps)
    }
  }

  def mkEvent(timestamps: Stream[Long] = tsStream)(implicit rnd: Random): Point = {
    Point.newBuilder()
      .setTimestamp(timestamps.head)
      .setIntValue(rnd.nextLong())
      .setMetric("proc.net.bytes")
      .addAttributes(Attribute.newBuilder()
      .setName("host")
      .setValue("localhost")
    ).build()
  }


}
