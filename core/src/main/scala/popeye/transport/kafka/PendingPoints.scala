package popeye.transport.kafka

import scala.concurrent.Promise
import java.io.ByteArrayOutputStream
import popeye.transport.proto.PackedPointsBuffer
import com.google.protobuf.{CodedOutputStream, CodedInputStream}
import java.util
import scala.collection.mutable
import scala.util.Random
import scala.annotation.tailrec
import com.typesafe.config.Config
import kafka.producer.ProducerConfig
import popeye.IdGenerator
import akka.actor._
import kafka.client.ClientUtils
import scala.concurrent.duration.FiniteDuration
import popeye.ConfigUtil._
import akka.actor.SupervisorStrategy.Restart
import scala.Some
import akka.actor.OneForOneStrategy
import popeye.transport.proto.PackedPointsIndex

/**
 * @author Andrey Stepachev
 */
object PendingPoints {

  final class PromiseForOffset(val offset: Int, val promise: Promise[Long])

  final class PartitionBuffer(val partitionId: Int, val points: Long, val buffer: Array[Byte])

  final class BufferForPartition(var consumed: Int = 0,
                                 var points: Long = 0,
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
      val bytes = buffer.toByteArray
      val coded = CodedInputStream.newInstance(bytes)
      var off = 0
      var cnt = 0
      while (!coded.isAtEnd) {
        cnt += 1
        val size = coded.readRawVarint32()
        val msgSize = size + CodedOutputStream.computeRawVarint32Size(size)
        if (off > 0 && off + msgSize > amount) {
          val bo = new ByteArrayOutputStream()
          bo.write(bytes, off, bytes.length - off)
          points -= cnt
          consumed += off
          return new PartitionBuffer(partitionId, cnt, util.Arrays.copyOf(bytes, off))
        }
        off += msgSize
      }
      throw new IllegalStateException("Reached end of buffer, possibly truncated buffer")
    }
  }

}

class PendingPoints(partitions: Int, minAmount: Int, maxAmount: Int) {

  import PendingPoints._

  private[this] val promises: Array[mutable.PriorityQueue[PromiseForOffset]] = Array.fill(partitions) {
    mutable.PriorityQueue[PromiseForOffset]()(Ordering.by(-_.offset))
  }
  private[this] val buffers: Array[BufferForPartition] = Array.fill(partitions) { new BufferForPartition() }
  private[this] var checkOrder = mkOrder

  private def mkOrder: List[Int] = {
    Random.shuffle((0 until partitions).toList)
  }

  def addPending(buffer: PackedPointsBuffer, maybePromise: Option[Promise[Long]]) = {
    val promiseOffsets = new Array[Int](partitions)
    buffer.foreach {
      pidx =>
        val partIdx: Int = Math.abs(pidx.hash) % partitions
        val pb: PendingPoints.BufferForPartition = buffers(partIdx)
        pb.append(pidx)
        promiseOffsets(partIdx) = pb.cumulativeOffset
    }

    maybePromise foreach (
      promise =>
        for (partition <- 0 until promiseOffsets.length if promiseOffsets(partition) > 0) {
          promises(partition).enqueue(new PromiseForOffset(promiseOffsets(partition), promise))
        }
      )
  }

  def consume(): (Seq[PartitionBuffer], Seq[Promise[Long]]) = {
    checkOrder = mkOrder
    (consumeInternal(checkOrder.iterator, 0, Seq()), consumePromises())
  }

  private def consumePromises(partition: Int): Seq[Promise[Long]] = {
    val bp = buffers(partition)
    val queue = promises(partition)
    var rv = Seq[Promise[Long]]()
    while (!queue.isEmpty) {
      val op = queue.dequeue()
      if (op.offset <= bp.consumed) {
        rv = rv :+ op.promise
      } else {
        queue.enqueue(op)
        return rv
      }
    }
    rv
  }

  private def consumePromises(): Seq[Promise[Long]] = {
    for {
      partition <- 0 until promises.length
      promise <- consumePromises(partition)
    } yield promise
  }

  @tailrec
  private def consumeInternal(partitions: Iterator[Int], total: Int, seq: Seq[PartitionBuffer]): Seq[PartitionBuffer] = {
    if (total > minAmount)
      return seq
    if (!partitions.hasNext)
      return seq
    val partitionIndex = partitions.next
    val pb = buffers(partitionIndex)
    if (pb.buffer.size < minAmount) {
      consumeInternal(partitions, total, seq)
    } else {
      val buffer = pb.consume(partitionIndex)
      consumeInternal(partitions, total + buffer.buffer.length, seq :+ buffer)
    }
  }

  def consumeOne(): Option[PartitionBuffer] = {
    for {
      partIdx <- checkOrder
      pb = buffers(partIdx)
      if pb.buffer.size >= minAmount
    } {
    }
    None
  }
}


