package popeye.transport.proto

import org.scalatest.FlatSpec
import popeye.test.PopeyeTestUtils._
import java.util.Random
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import PointsQueue._

/**
 * @author Andrey Stepachev
 */
class PendingPointsTestSpec extends FlatSpec {


  behavior of "PointsQueue with 3 partition"

  it should "partial consume should work" in {
    val (p1, pr1) = PackedPoints(mkEvents(10)) -> Promise[Long]()
    val (p2, pr2) = PackedPoints(mkEvents(10)) -> Promise[Long]()
    val pp = new PointsQueue(3, 100, p1.pointsBuffer.length)
    checkEmptyPP(pp)

    pp.addPending(p1, pr1)
    pp.addPending(p2, pr2)
    val stat1 = pp.stat

    val (buffers, promises) = validPP(pp).consume()
    assert(buffers.size == 2)
    assert(promises.size == 0)
    promises.foreach(_.success(1))

    val stat11 = countStat(buffers, promises)
    val stat2 = pp.stat
    assert(stat2 + stat11 === stat1, s"$stat2 + $stat11 === $stat1")

    val (buffers2, promises2) = validPP(pp).consume(ignoreMinAmount = true)
    assert(buffers2.size == 1)
    assert(promises2.size == 2)
    promises2.foreach(_.success(1))

    val stat22 = countStat(buffers2, promises2)
    val stat3 = pp.stat
    assert(stat3 + stat22 === stat2, s"$stat3 + $stat22 === $stat2")

    val (buffers3, promises3) = validPP(pp).consume(ignoreMinAmount = true)
    assert(buffers3.size == 0)
    assert(promises3.size == 0)
    promises3.foreach(_.success(1))

    val stat33 = countStat(buffers3, promises3)
    val stat4 = pp.stat
    assert(stat4 + stat33 === stat3, s"$stat4 + $stat33 === $stat3")

    checkEmptyPP(pp)

    assert(pr1.isCompleted)
    assert(pr2.isCompleted)
  }

  behavior of "PointsQueue with 1 partition"

  it should "consume whole buffer" in {
    val pp = new PointsQueue(1, 1, Int.MaxValue)
    val pr = Promise[Long]()
    pp.addPending(points, pr)
    val (buffer, promises) = validPP(pp).consume()
    promises.foreach(_.success(1))
    assert(buffer.size == 1)
    assert(buffer(0).buffer.size == points.pointsBuffer.length)
    val r = Await.result(pr.future, 1 seconds)
    assert(r == 1)
    checkEmptyPP(pp)
  }

  it should "none should be consumed if less them minimum available" in {
    val pp = new PointsQueue(1, points.pointsBuffer.length + 1, Int.MaxValue)
    val pr = Promise[Long]()
    pp.addPending(points, pr)

    val stat1 = pp.stat
    val (buffers, promises) = validPP(pp).consume()
    assert(buffers.size == 0)
    assert(promises.size == 0)
    promises.foreach(_.success(1))
    val stat11 = countStat(buffers, promises)
    val stat2 = pp.stat
    assert(stat2 + stat11 === stat1, s"$stat2 + $stat11 === $stat1")

    pp.consume(ignoreMinAmount = true)
    checkEmptyPP(pp)
  }

  it should "partial consume should work" in {
    val (p1, pr1) = PackedPoints(mkEvents(10)) -> Promise[Long]()
    val (p2, pr2) = PackedPoints(mkEvents(10)) -> Promise[Long]()
    val pp = new PointsQueue(1, p1.pointsBuffer.length, p1.pointsBuffer.length)
    checkEmptyPP(pp)

    pp.addPending(p1, pr1)
    pp.addPending(p2, pr2)
    val stat1 = pp.stat

    val (buffers, promises) = validPP(pp).consume()
    assert(buffers.size == 1)
    assert(promises.size == 1)
    promises.foreach(_.success(1))

    val stat11 = countStat(buffers, promises)
    val stat2 = pp.stat
    assert(stat2 + stat11 === stat1, s"$stat2 + $stat11 === $stat1")

    val (buffers2, promises2) = validPP(pp).consume(ignoreMinAmount = true)
    assert(buffers2.size == 1)
    assert(promises2.size == 0)
    promises2.foreach(_.success(1))

    val stat22 = countStat(buffers2, promises2)
    val stat3 = pp.stat
    assert(stat3 + stat22 === stat2, s"$stat3 + $stat22 === $stat2")

    val (buffers3, promises3) = validPP(pp).consume(ignoreMinAmount = true)
    assert(buffers3.size == 1)
    assert(promises3.size == 1)
    promises3.foreach(_.success(1))

    val stat33 = countStat(buffers3, promises3)
    val stat4 = pp.stat
    assert(stat4 + stat33 === stat3, s"$stat4 + $stat33 === $stat3")

    checkEmptyPP(pp)
  }


  //  "PendingPoinst" should "has good performance" in {
  //    val rr = new MetricRegistry()
  //    val points = mkEvents(10000000)
  //    info("Done generating, running test")
  //    val pointsMeter = rr.meter("points")
  //    val batchesMeter = rr.meter("batches")
  //    val batchesSizes = rr.histogram("batcheSizes")
  //    Range(6000, 9000, 3000) foreach {
  //      iter =>
  //        val pp = new PointsQueue(100, 100, iter * 10)
  //        pp.addPending(PackedPoints(points.take(100000)), None)
  //        var exit: Boolean = false
  //        var additions = 1
  //        while(!exit) {
  //          val buffer = {
  //            val (b, _) = pp.consume()
  //            if (b.isEmpty)
  //              pp.consume(ignoreMinAmount = true)._1
  //            else
  //              b
  //          }
  //          if (buffer.isEmpty)
  //            exit = true
  //          buffer.map {
  //            b =>
  //              batchesSizes.update(b.points)
  //          }
  //          batchesMeter.mark(buffer.size)
  //          pointsMeter.mark(countPoints(buffer))
  //          if (additions > 0 && additions < points.length) {
  //            pp.addPending(PackedPoints(points.drop(rnd.nextInt(10)*10000).take(100000)), None)
  //            additions -= 1
  //          }
  //        }
  //    }
  //    ConsoleReporter.forRegistry(rr).build().report()
  //    assert(true)
  //  }


  implicit val rnd = new Random(1234)

  val points = PackedPoints(mkEvents(10))

  def countStat(buffers: Seq[PartitionBuffer], promises: Seq[Promise[_]]): Stat = {
    Stat(countBytes(buffers), countPoints(buffers), promises.length)
  }

  def countPoints(buffers: Seq[PartitionBuffer]): Long = buffers.foldLeft(0l) { (a, b) => a + b.points }

  def countBytes(buffers: Seq[PartitionBuffer]): Long = buffers.foldLeft(0l) { (a, b) => a + b.buffer.length }

  def checkEmptyPP(pp: PointsQueue): Unit = {
    val stat = pp.stat
    assert(stat match {
      case Stat(0, 0, 0) => true
      case _ => false
    }, s"Should be empty $stat")
  }

  def validPP(pp: PointsQueue): PointsQueue = {
    val stat = pp.stat
    assert(stat match {
      case Stat(a, b, c) if a >= 0 && b >= 0 && c >= 0 => true
      case _ => false
    }, s"Invalid $stat")
    pp
  }

}
