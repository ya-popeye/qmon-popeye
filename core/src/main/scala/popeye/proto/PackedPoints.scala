package popeye.proto

import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import java.io.OutputStream
import popeye.proto.Message.Point
import scala.collection.mutable.ArrayBuffer

/**
 * @author Andrey Stepachev
 */
final class PackedPoints(val avgMessageSize: Int = 100, val messagesPerExtent: Int = 100, val initialCapacity: Int = 0) {
  private[proto] val points = new ExpandingBuffer(initialMessages * avgMessageSize)
  private val pointsCoder = CodedOutputStream.newInstance(points)
  private var pointsCount_ = 0

  private def initialMessages = if (initialCapacity > 0) initialCapacity else messagesPerExtent

  def pointsCount = pointsCount_

  def bufferLength = {
    pointsCoder.flush()
    points.length
  }


  @inline
  def +=(point: Point): PackedPoints = append(point)

  @inline
  def ++=(points: Seq[Point]): PackedPoints = append(points: _*)

  def append(point: Point): PackedPoints = {
    MessageUtil.validatePoint(point)
    val size = point.getSerializedSize
    pointsCoder.writeRawVarint32(size)
    point.writeTo(pointsCoder)
    pointsCount_ += 1
    this
  }

  def append(points: Point*): PackedPoints = {
    points.foreach { point => append(point)}
    this
  }

  def consume(amount: Int): PackedPoints = {
    val p = new PackedPoints()
    p.consumeFrom(this, amount)
    p
  }

  def copyOfBuffer = {
    pointsCoder.flush()
    points.toByteArray
  }

  override def clone(): PackedPoints = {
    val copy = new PackedPoints()
    copy.pointsCoder.writeRawBytes(copy.buffer)
    copy.pointsCount_ = this.pointsCount_
    copy
  }

  def consumeFrom(pp: PackedPoints, pointsToConsume: Int) = {
    require(pp.pointsCount >= pointsToConsume, "Nonempty pack required")
    val cs = CodedInputStream.newInstance(pp.buffer, pp.bufferOffset, pp.bufferLength)
    var readSize = 0
    for (message <- 0 until pointsToConsume) {
      val size = cs.readRawVarint32()
      cs.skipRawBytes(size)
      readSize += size + CodedOutputStream.computeRawVarint32Size(size)
    }
    pointsCoder.writeRawBytes(pp.buffer, pp.bufferOffset, readSize)
    pointsCoder.flush()
    pointsCount_ += pointsToConsume
    pp.consumeBytes(readSize)
    pp.pointsCount_ -= pointsToConsume
    this
  }

  def foreach[U](f: (Point) => U): Unit = {
    val cs = CodedInputStream.newInstance(buffer, bufferOffset, bufferLength)
    for (message <- 0 until pointsCount) {
      val size = cs.readRawVarint32()
      val limit = cs.pushLimit(size)
      val point = Point.newBuilder().mergeFrom(cs).build
      cs.popLimit(limit)
      f(point)
    }
  }

  def map[U](f: (Point) => U): Seq[U] = {
    val cs = CodedInputStream.newInstance(buffer, bufferOffset, bufferLength)
    for (message <- 0 until pointsCount) yield {
      val size = cs.readRawVarint32()
      val limit = cs.pushLimit(size)
      val point = Point.newBuilder().mergeFrom(cs).build
      cs.popLimit(limit)
      f(point)
    }
  }

  def toPointsSeq: Seq[Point] = {
    map(p => p)
  }

  private[proto] def buffer = {
    pointsCoder.flush()
    points.buffer
  }

  private[proto] def bufferOffset = {
    pointsCoder.flush()
    points.offset
  }

  private[proto] def consumeBytes(amount: Int) = {
    pointsCoder.flush()
    points.consume(amount)
  }

}

object PackedPoints {

  val expectedMessageSize = 50

  def prependBatchId(batchId: Long, array: Array[Byte]): Array[Byte] = {
    val longSize = CodedOutputStream.computeInt64SizeNoTag(batchId)
    val b = new ExpandingBuffer(longSize + array.length)
    val cs = CodedOutputStream.newInstance(b)
    cs.writeInt64NoTag(batchId)
    cs.writeRawBytes(array)
    cs.flush()
    b.toByteArray
  }

  def decodePoints(buffer: Array[Byte]): Seq[Point] = {
    val cs = CodedInputStream.newInstance(buffer)
    val points = decodePoints(cs, buffer.length / expectedMessageSize)
    points
  }


  def decodeWithBatchId(buffer: Array[Byte]): (Long, Seq[Point]) = {
    val cs = CodedInputStream.newInstance(buffer)
    val batchId = cs.readInt64()
    val points = decodePoints(cs, buffer.length / expectedMessageSize)
    (batchId, points)
  }

  def decodePoints(cs: CodedInputStream, sizeHint: Int): Seq[Point] = {
    val points = new ArrayBuffer[Point]()
    while (!cs.isAtEnd) {
      val size = cs.readRawVarint32()
      val limit = cs.pushLimit(size)
      points += Point.newBuilder().mergeFrom(cs).build
      cs.popLimit(limit)
    }
    points
  }

  @inline
  def apply(): PackedPoints = {
    new PackedPoints
  }

  @inline
  def apply(messagesPerExtent: Int): PackedPoints = {
    new PackedPoints(messagesPerExtent = messagesPerExtent)
  }

  @inline
  def apply(points: Seq[Point]): PackedPoints = {
    val pack = new PackedPoints(initialCapacity = points.size)
    points foreach pack.append
    pack
  }
}
