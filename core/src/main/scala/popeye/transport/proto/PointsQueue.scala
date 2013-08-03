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

  final class PromiseForOffset(val offset: Int, val promise: Promise[Long], val shares: AtomicInteger)

  final class PartitionBuffer(val partitionId: Int, val points: Long, val buffer: Array[Byte])

  final class BufferForPartition(
                                  val partitionId: Int,
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

    def consume(partitionId: Int): PartitionBuffer = {
      time = System.currentTimeMillis()
      val bytes = buffer.toByteArray
      val r = new PartitionBuffer(partitionId, points, bytes)
      consumed += buffer.size()
      points = 0
      buffer.reset()
      r
    }

    def consume(partitionId: Int, amount: Int): PartitionBuffer = {
      if (amount >= buffer.size)
        return consume(partitionId)
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
          log.debug(point.toString)
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
          return new PartitionBuffer(partitionId, cnt, util.Arrays.copyOf(bytes, off))
        }
        cnt += 1
        off += msgSize
      }
      throw new IllegalStateException("Reached end of buffer, possibly truncated buffer")
    }
  }

}

class PointsQueue(partitions: Int, minAmount: Int, maxAmount: Int, timeStep: Long = (100 millis).toMillis) {

  import PointsQueue._

  private[this] val promises: Array[mutable.PriorityQueue[PromiseForOffset]] = Array.fill(partitions) {
    mutable.PriorityQueue[PromiseForOffset]()(Ordering.by(-_.offset))
  }
  private[this] val buffers: Array[BufferForPartition] = {
    val arr = new Array[BufferForPartition](partitions)
    var i = 0
    while (i < partitions) {
      arr(i) = new BufferForPartition(i)
      i += 1
    }
    arr
  }


  private def mkOrder = {
    val pq = mutable.PriorityQueue[BufferForPartition]()(Ordering.by({
      bp =>
        if (bp.points == 0) {
          0
        } else {
          val now = System.currentTimeMillis()
          val step = (now - bp.time) % 1000000 / timeStep // take 1000 secs for ordering steps
          step << 24 | bp.points
        }
    }))
    pq ++= buffers
    pq
  }

  def stat: Stat = {
    val bufferStat = buffers.foldLeft((0l /* points */ , 0 /* bytes */ )) {
      (accum, bp) => (accum._1 + bp.points, accum._2 + bp.buffer.size())
    }
    val promisesStat = promises.flatMap {a => a}.map {_.promise}.distinct.size
    Stat(bufferStat._2, bufferStat._1, promisesStat)
  }

  @inline
  def addPending(buffer: PackedPointsBuffer, promise: Promise[Long]): Unit = {
    addPending(buffer, Some(promise))
  }

  def addPending(buffer: PackedPointsBuffer, maybePromise: Option[Promise[Long]]): Unit = {
    val promiseOffsets = new Array[Int](partitions)
    buffer.foreach {
      pidx =>
        val partIdx: Int = Math.abs(pidx.hash) % partitions
        val pb: PointsQueue.BufferForPartition = buffers(partIdx)
        pb.append(pidx)
        promiseOffsets(partIdx) = pb.cumulativeOffset
    }

    val shares = new AtomicInteger(0)
    maybePromise foreach (
      promise =>
        for (partition <- 0 until promiseOffsets.length if promiseOffsets(partition) > 0) {
          shares.incrementAndGet()
          promises(partition).enqueue(new PromiseForOffset(promiseOffsets(partition), promise, shares))
        }
      )
  }

  def consume(ignoreMinAmount: Boolean = false): (Seq[PartitionBuffer], Seq[Promise[Long]]) = {
    (consumeInternal(ignoreMinAmount, mkOrder.toIterator, 0, Seq()), consumePromises())
  }

  private def consumePromises(): Seq[Promise[Long]] = {
    var rv = ArrayBuffer[Promise[Long]]()
    var partition = 0
    while (partition < promises.length) {
      def traverseQueue() {
        val bp = buffers(partition)
        val queue = promises(partition)
        while (!queue.isEmpty) {
          val op = queue.dequeue()
          if (op.offset <= bp.consumed) {
            val shares = op.shares.decrementAndGet()
            require(shares >= 0)
            if (shares == 0)
              rv += op.promise
          } else {
            queue.enqueue(op)
            return
          }
        }
      }
      traverseQueue()
      partition += 1
    }
    rv
  }

  @tailrec
  private def consumeInternal(ignoreMinAmount: Boolean, partitions: Iterator[BufferForPartition], total: Int, seq: Seq[PartitionBuffer]): Seq[PartitionBuffer] = {
    if (total >= maxAmount)
      return seq
    if (!partitions.hasNext)
      return seq
    val pb = partitions.next()
    if ((ignoreMinAmount && pb.buffer.size > 0) || pb.buffer.size >= minAmount) {
      val buffer = pb.consume(pb.partitionId, maxAmount)
      consumeInternal(ignoreMinAmount, partitions, total + buffer.buffer.length, seq :+ buffer)
    } else {
      consumeInternal(ignoreMinAmount, partitions, total, seq)
    }
  }
}


