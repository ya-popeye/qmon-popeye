package popeye.transport.proto

import scala.concurrent.Promise
import java.io.ByteArrayOutputStream
import com.google.protobuf.{CodedOutputStream, CodedInputStream}
import java.util
import scala.collection.mutable
import scala.annotation.tailrec
import popeye.Logging
import popeye.transport.proto.Message.Point
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Andrey Stepachev
 */
object PointsQueue extends Logging {

  case class Stat(bytes: Long, points: Long, promises: Int) {
    def +(other: Stat) = Stat(bytes + other.bytes, points + other.points, promises + other.promises)
  }

  final class PromiseForOffset(val offset: Int, val promise: Promise[Long])

  final class PartitionBuffer(val points: Long, val buffer: Array[Byte])

  final class BufferedPoints(
                              var consumed: Int = 0,
                              var points: Long = 0,
                              var time: Long = 0,
                              val buffer: ByteArrayOutputStream = new ByteArrayOutputStream()) {

    def cumulativeOffset = consumed + buffer.size

    @inline
    def append(pidx: PackedPointsIndex): Unit = {
      append(1, pidx.delimitedPoints, pidx.offset, pidx.len)
    }

    def append(containedPoints: Int, data: Array[Byte], offset: Int, len: Int): Unit = {
      buffer.write(data, offset, len)
      points += containedPoints
    }

    def consumeAll(): PartitionBuffer = {
      time = System.currentTimeMillis()
      val bytes = buffer.toByteArray
      val r = new PartitionBuffer(points, bytes)
      consumed += buffer.size()
      points = 0
      buffer.reset()
      r
    }

    def consume(amount: Int): PartitionBuffer = {
      if (amount >= buffer.size)
        return consumeAll()
      time = System.currentTimeMillis()
      val bytes = buffer.toByteArray
      val coded = CodedInputStream.newInstance(bytes)
      var off = 0
      var cnt = 0
      while (!coded.isAtEnd) {
        val size = coded.readRawVarint32()
        if (false) {
          val plimit = coded.pushLimit(size)
          val point: Point = Point.newBuilder().mergeFrom(coded).build
          debug(point.toString)
          coded.popLimit(plimit)
        } else {
          coded.skipRawBytes(size)
        }
        val msgSize = size + CodedOutputStream.computeRawVarint32Size(size)
        if (off > 0 && off + msgSize > amount) {
          buffer.reset()
          buffer.write(bytes, off, bytes.length - off)
          points -= cnt
          consumed += off
          return new PartitionBuffer(cnt, util.Arrays.copyOf(bytes, off))
        }
        cnt += 1
        off += msgSize
      }
      throw new IllegalStateException("Reached end of buffer, possibly truncated buffer")
    }
  }

}

class PointsQueue(minAmount: Int, maxAmount: Int) {

  import PointsQueue._

  private[this] val promises: mutable.PriorityQueue[PromiseForOffset] = mutable.PriorityQueue[PromiseForOffset]()(Ordering.by(-_.offset))
  private[this] val points: BufferedPoints = new BufferedPoints()

  def stat: Stat = {
    Stat(points.points, points.buffer.size, promises.size)
  }

  @inline
  def addPending(buffer: PackedPointsBuffer, promise: Promise[Long]): Unit = {
    addPending(buffer, Some(promise))
  }

  def addPending(buffer: PackedPointsBuffer, maybePromise: Option[Promise[Long]]): Unit = {
    buffer.foreach {
      pidx =>
        points.append(pidx)
    }
    maybePromise foreach (
      promise =>
        promises.enqueue(new PromiseForOffset(points.cumulativeOffset, promise))
      )
  }

  def consume(ignoreMinAmount: Boolean = false): (Option[PartitionBuffer], Seq[Promise[Long]]) = {
    val buffer = consumeInternal(ignoreMinAmount)
    if (buffer.isDefined) {
      (buffer, consumePromises())
    } else {
      (buffer, Seq.empty)
    }
  }

  private def consumeInternal(ignoreMinAmount: Boolean): Option[PartitionBuffer] = {
    if ((ignoreMinAmount && points.buffer.size > 0) || points.buffer.size >= minAmount) {
      Some(points.consume(maxAmount))
    } else {
      None
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


