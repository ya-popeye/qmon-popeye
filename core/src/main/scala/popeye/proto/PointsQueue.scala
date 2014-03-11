package popeye.proto

import popeye.Logging
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise

/**
 * @author Andrey Stepachev
 */
object PointsQueue extends Logging {

  case class Stat(points: Long, batches: Long, promises: Int) {
    def +(other: Stat) = Stat(points + other.points, batches + other.batches, promises + other.promises)
  }

  final class PromiseForOffset(val offset: Long, val promise: Promise[Long])

  final class BufferedPoints(var consumed: Long = 0,
                             var pointsCount: Int = 0,
                             var time: Long = 0) {

    var buffer = new mutable.ArrayBuffer[PackedPoints]
    def cumulativeOffset = consumed + pointsCount

    def append(pack: PackedPoints): Unit = {
      buffer += pack
      pointsCount += pack.pointsCount
      trace(s"added pack=${pack.pointsCount} pc=$pointsCount, buffers=${buffer.map{_.pointsCount}}")
    }

    private def takeBuffer() = {
      val r = buffer
      buffer = new mutable.ArrayBuffer[PackedPoints]
      consumed += pointsCount
      pointsCount = 0
      r
    }

    def consumeAll(): Seq[PackedPoints] = {
      time = System.currentTimeMillis()
      takeBuffer()
    }

    private def findPackWhichFit(amount: Int): Int = {
      var toGo = amount
      var idx = 0
      while (toGo > 0 && idx < buffer.length) {
        trace(s"toGo=$toGo, idx=$idx, pc=$pointsCount, buffers=${buffer.map{_.pointsCount}}")
        if (buffer(idx).pointsCount > toGo) {
          return idx
        } else {
          toGo -= buffer(idx).pointsCount
          idx += 1
          trace(s"next iteration toGo=$toGo, idx=$idx, buffers=${buffer.map{_.pointsCount}}")
        }
      }
      if (toGo > 0)
        throw new IllegalStateException(s"Reached end of buffer, trying to consume $amount out of $pointsCount")
      else
        idx
    }

    def consume(amount: Int): Seq[PackedPoints] = {
      if (pointsCount == 0)
        return Seq()
      if (amount >= pointsCount)
        return consumeAll()
      time = System.currentTimeMillis()
      val idx = findPackWhichFit(amount)
      consumed += amount
      pointsCount -= amount
      if (idx == 0) {
        val r = Seq(buffer.head.consume(amount))
        trace(s"simple consume amount=$amount idx=$idx, buffers=${buffer.map{_.pointsCount}}")
        r
      } else {
        val packs = buffer.take(idx) // take first packs as is
        buffer.remove(0, idx) // remove them
        val consumeFromLast = amount - packs.foldLeft(0){(acc, v) => acc + v.pointsCount}
        if (consumeFromLast > 0) {
          packs += buffer.head.consume(consumeFromLast)
        }
        trace(s"consume amount=$amount idx=$idx, buffers=${buffer.map{_.pointsCount}}")
        packs
      }
    }
  }

}

class PointsQueue(minAmount: Int, maxAmount: Int) {

  import PointsQueue._

  private[this] val promises = mutable.PriorityQueue[PromiseForOffset]()(Ordering.by(-_.offset))
  private[this] val points = new BufferedPoints()

  def stat: Stat = {
    Stat(points.pointsCount, points.buffer.size, promises.size)
  }

  @inline
  def addPending(buffer: PackedPoints, promise: Promise[Long]): Unit = {
    addPending(buffer, Some(promise))
  }

  def addPending(buffer: PackedPoints, maybePromise: Option[Promise[Long]]): Unit = {
    points.append(buffer)
    maybePromise foreach (
      promise =>
        promises.enqueue(new PromiseForOffset(points.cumulativeOffset, promise))
      )
  }

  def consume(ignoreMinAmount: Boolean = false): (Seq[PackedPoints], Seq[Promise[Long]]) = {
    val buffer = consumeInternal(ignoreMinAmount)
    if (!buffer.isEmpty) {
      (buffer, consumePromises())
    } else {
      (buffer, Seq.empty)
    }
  }

  private def consumeInternal(ignoreMinAmount: Boolean): Seq[PackedPoints] = {
    if ((ignoreMinAmount && points.pointsCount > 0) || points.pointsCount >= minAmount) {
      points.consume(maxAmount)
    } else {
      Seq.empty
    }
  }

  private def consumePromises(): Seq[Promise[Long]] = {
    var rv = ArrayBuffer[Promise[Long]]()
    while (!promises.isEmpty) {
      val op = promises.dequeue()
      if (op.offset <= points.consumed) {
        rv += op.promise
      } else {
        promises.enqueue(op)
        return rv // ok, first promise out of cumulative offset, stop iteration for now
      }
    }
    rv
  }

}


