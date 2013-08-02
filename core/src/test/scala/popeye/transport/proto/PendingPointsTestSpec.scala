package popeye.transport.proto

import org.scalatest.FlatSpec
import popeye.test.PopeyeTestUtils._
import java.util.Random
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

/**
 * @author Andrey Stepachev
 */
class PendingPointsTestSpec extends FlatSpec {

  behavior of "PendingPoints with 1 partition"

  it should "none should partially consume" in {
    val (p1, pr1) = PackedPoints(mkEvents(10)) -> Promise[Long]()
    val (p2, pr2) = PackedPoints(mkEvents(10)) -> Promise[Long]()
    val pp = new PendingPoints(1, p1.pointsBuffer.length, p1.pointsBuffer.length)
    pp.addPending(p1, pr1)
    pp.addPending(p2, pr2)
    val (buffer, promises) = pp.consume()
    assert(buffer.size == 1)
    assert(promises.size == 1)
  }

  it should "consume whole buffer" in {
    val pp = new PendingPoints(1, 1, Int.MaxValue)
    val pr = Promise[Long]()
    pp.addPending(points, pr)
    val (buffer, promises) = pp.consume()
    promises.foreach(_.success(1))
    assert(buffer.size == 1)
    assert(buffer(0).buffer.size == points.pointsBuffer.length)
    val r = Await.result(pr.future, 1 seconds)
    assert(r == 1)
  }

  it should "none should be consumed if less them minimum available" in {
    val pp = new PendingPoints(1, points.pointsBuffer.length + 1, Int.MaxValue)
    val pr = Promise[Long]()
    pp.addPending(points, pr)
    val (buffer, promises) = pp.consume()
    assert(buffer.size == 0)
    assert(promises.size == 0)
  }

  implicit val rnd = new Random(1234)
  val points = PackedPoints(mkEvents(10))
}
