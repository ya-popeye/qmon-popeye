package popeye.proto

import PointsQueue._
import java.util.Random
import org.scalatest.{Matchers, FlatSpec}
import popeye.test.PopeyeTestUtils._
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.collection.mutable.ArrayBuffer
import popeye.proto.Message.Point
import popeye.Logging

/**
 * @author Andrey Stepachev
 */
class PointsQueueSpec extends FlatSpec with Matchers with Logging {


  behavior of "PointsQueue"

  it should "consume whole buffer" in {
    val pp = new PointsQueue(1, Int.MaxValue)
    val pr = Promise[Long]()
    pp.addPending(points, pr)
    val (buffer, promises) = validPP(pp).consume()
    promises.foreach(_.success(1))
    assert(buffer.size == 1)
    assert(buffer.foldLeft(0) { (acc, v) => acc + v.pointsCount} == points.pointsCount)
    val r = Await.result(pr.future, 1 seconds)
    assert(r == 1)
    checkEmptyPP(pp)
  }

  it should "none should be consumed if less them minimum available" in {
    val pp = new PointsQueue(points.bufferLength + 1, Int.MaxValue)
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

  it should "integration test" in {
    debug("Done generating, running test")
    List(2, 117, 1700) foreach {
      iter =>
        val points = mkEvents(10 * iter)
        val resultingPoints = new ArrayBuffer[Point]()
        var idx = 0
        debug(s"Iter $iter")
        val pp = new PointsQueue(iter, iter*1.2.toInt)
        pp.addPending(PackedPoints(points.slice(idx, idx + iter)), None)
        idx += iter
        var exit: Boolean = false
        while (!exit) {
          val batch = {
            val (b, _) = pp.consume()
            if (b.isEmpty)
              pp.consume(ignoreMinAmount = true)._1
            else
              b
          }
          if (batch.isEmpty)
            exit = true
          else {
            resultingPoints ++= (for{
              pack <- batch
              point <- pack
            } yield point)
            if (idx < points.size) {
              val sliceSize = Math.min(points.size - idx, rnd.nextInt(iter) + 1)
              pp.addPending(PackedPoints(points.slice(idx, idx + sliceSize)), None)
              idx += sliceSize
            }
          }
        }
        resultingPoints should be (points)
    }
  }

  implicit val rnd = new Random(1234)

  val points = PackedPoints(mkEvents(10))

  def countStat(buffer: Seq[PackedPoints], promises: Seq[Promise[_]]): Stat = {
    if (!buffer.isEmpty) {
      val bytes = buffer.foldLeft(0) { (acc, v) => acc + v.bufferLength}
      val points = buffer.foldLeft(0) { (acc, v) => acc + v.pointsCount}
      Stat(bytes, points, promises.length)
    } else
      Stat(0, 0, promises.length)
  }

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
