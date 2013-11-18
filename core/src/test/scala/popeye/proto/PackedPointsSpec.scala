package popeye.proto

import java.util.Random
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import popeye.test.PopeyeTestUtils._

/**
 * @author Andrey Stepachev
 */
class PackedPointsSpec extends FlatSpec with ShouldMatchers {


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
    points.toPointsSeq.zip(expected).foreach{pair=>
      pair._1 should be (pair._2)
    }
    points.consume(1).toPointsSeq should be(expected.take(1))
  }

  it should "issue" in {
    val events = mkEvents(117)
    val points = PackedPoints(events)
    val rpoints = new PackedPoints
    for (i <- 0 until 117) yield {
      points.consume(1)
    }.foreach(rpoints.append)
    rpoints.toPointsSeq.zip(events).foreach{pair=>
      pair._1 should be (pair._2)
    }
  }

  implicit val rnd = new Random(1234)
}
