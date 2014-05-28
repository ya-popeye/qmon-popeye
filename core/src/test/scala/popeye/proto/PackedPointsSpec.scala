package popeye.proto

import java.util.Random
import org.scalatest.{Matchers, FlatSpec}
import popeye.test.PopeyeTestUtils._
import com.google.protobuf.CodedOutputStream
import java.io.ByteArrayOutputStream

/**
 * @author Andrey Stepachev
 */
class PackedPointsSpec extends FlatSpec with Matchers {


  behavior of "PackedPoints"

  it should "consume" in {
    val events = mkEvents(10)
    val points = PackedPoints(events)
    val p1 = points.consume(1)
    val p2 = points.consume(2)
    p1.foreach { p =>
      points.append(p)
    }
    p2.foreach { p =>
      points.append(p)
    }
    val expected = events.drop(3) ++ events.take(3)
    points.zip(expected).foreach{pair=>
      pair._1 should be (pair._2)
    }
    points.consume(1).toSeq should be(expected.take(1))
  }

  it should "issue" in {
    val events = mkEvents(117)
    val points = PackedPoints(events)
    val rpoints = PackedPoints()
    for (i <- 0 until 117) yield {
      points.consume(1)
    }.foreach(rpoints.append)
    rpoints.zip(events).foreach{pair=>
      pair._1 should be (pair._2)
    }
  }

  behavior of "PackedPoints.fromBytes"

  it should "create PackedPoints form byte array" in {
    val points = (0 to 10).map(i =>
      createPoint(
        metric = f"test$i",
        timestamp = i,
        attributes = Seq(f"a$i" -> f"b$i"),
        value = Left(i.toLong)
      )
    )
    val bytes = toBytes(points)
    val packedPoints = PackedPoints.fromBytes(bytes)
    packedPoints.toSeq should equal(points)
  }

  def toBytes(points: Seq[Message.Point]) = {
    val byteArrayOutStream = new ByteArrayOutputStream()
    val codedStream = CodedOutputStream.newInstance(byteArrayOutStream)
    for (point <- points) {
      val size = point.getSerializedSize
      codedStream.writeRawVarint32(size)
      point.writeTo(codedStream)
    }
    codedStream.flush()
    byteArrayOutStream.toByteArray
  }

  implicit val rnd = new Random(1234)

  ignore should "have good performance" in {
    val points = (0 to 100000).map(i =>
      createPoint(
        metric = f"test_metric$i",
        timestamp = i,
        attributes = (0 to 3).map(i => f"name_$i" -> f"value_$i"),
        value = Left(i.toLong)
      )
    )
    val bytes = toBytes(points)
    for (_ <- 0 to 10) {
      val time = runTime {PackedPoints.fromBytes(bytes)}
      println(time)
    }
  }

  def runTime(block: => Unit) = {
    val startTime = System.currentTimeMillis()
    block
    System.currentTimeMillis() - startTime
  }
}
