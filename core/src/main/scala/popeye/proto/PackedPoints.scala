package popeye.proto

import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import java.io.OutputStream
import popeye.proto.Message.Point
import scala.collection.mutable.ArrayBuffer

/**
 * @author Andrey Stepachev
 */
final class PackedPoints(points: ExpandingBuffer, numberOfPointsInBuffer: Int = 0) extends scala.collection.Iterable[Point] {
  private val pointsCoder = CodedOutputStream.newInstance(points)
  private var pointsCount_ = numberOfPointsInBuffer

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
    val p = PackedPoints()
    p.consumeFrom(this, amount)
    p
  }

  def copyOfBuffer = {
    pointsCoder.flush()
    points.toByteArray
  }

  override def clone(): PackedPoints = {
    val copy = PackedPoints()
    copy.pointsCoder.writeRawBytes(copy.buffer)
    copy.pointsCount_ = this.pointsCount_
    copy
  }

  def consumeFrom(pp: PackedPoints, pointsToConsume: Int) = {
    require(pp.pointsCount >= pointsToConsume, "not enough points")
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

  override def iterator: Iterator[Point] = new Iterator[Point] {
    var index = 0
    val cs = CodedInputStream.newInstance(buffer, bufferOffset, bufferLength)

    override def next(): Point = {
      index += 1
      val size = cs.readRawVarint32()
      val limit = cs.pushLimit(size)
      val point = Point.newBuilder().mergeFrom(cs).build
      cs.popLimit(limit)
      point
    }

    override def hasNext: Boolean = index < pointsCount_
  }

  override def size: Int = pointsCount_
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
    val points = decodePoints(cs)
    points
  }

  def decodeWithBatchId(buffer: Array[Byte]): (Long, Seq[Point]) = {
    val cs = CodedInputStream.newInstance(buffer)
    val batchId = cs.readInt64()
    val points = decodePoints(cs)
    (batchId, points)
  }

  def decodePoints(cs: CodedInputStream): Seq[Point] = {
    val points = new ArrayBuffer[Point]()
    while (!cs.isAtEnd) {
      val size = cs.readRawVarint32()
      val limit = cs.pushLimit(size)
      points += Point.newBuilder().mergeFrom(cs).build
      cs.popLimit(limit)
    }
    points
  }

  def fromBytes(bytes: Array[Byte]): PackedPoints = {
    val pointsCount = decodePoints(bytes).size
    val buffer = new ExpandingBuffer(bytes.size)
    buffer.write(bytes)
    new PackedPoints(buffer, pointsCount)
  }

  @inline
  def apply(): PackedPoints = {
    PackedPoints(sizeHint = 100)
  }

  @inline
  def apply(sizeHint: Int): PackedPoints = {
    val avgMessageSize = 100
    val points = new ExpandingBuffer(sizeHint * avgMessageSize)
    new PackedPoints(points)
  }

  @inline
  def apply(points: Iterable[Point]): PackedPoints = {
    val pack = PackedPoints(points.size)
    points foreach pack.append
    pack
  }
}
