package popeye.transport.proto

import popeye.transport.proto.Message.Point
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import java.io.OutputStream
import scala.collection.mutable.ArrayBuffer
import java.util

/**
 * @author Andrey Stepachev
 */
class PackedPoints(avgMessageSize: Int = 100, messagesPerExtent: Int = 100) {
  private[this] val hashes = new ExpandingBuffer(messagesPerExtent * 8)
  private[this] val hashesCoder = CodedOutputStream.newInstance(hashes)
  private[this] val points = new ExpandingBuffer(messagesPerExtent * avgMessageSize)
  private[this] val pointsCoder = CodedOutputStream.newInstance(points)
  var cumulativeHash: Int = 0
  var pointsCount: Int = 0

  @inline
  def +=(point: Point) = append(point)

  def append(point: Point) = {
    MessageUtil.validatePoint(point)
    val hash: Int = point.getMetric.hashCode
    hashesCoder.writeInt32NoTag(hash)
    val size = point.getSerializedSize
    pointsCoder.writeRawVarint32(size)
    point.writeTo(pointsCoder)
    pointsCount += 1
    cumulativeHash ^= hash
    this
  }

  private[proto] def pointsBuffer = {
    pointsCoder.flush()
    points.toByteArray
  }

  private[proto] def hashesBuffer = {
    hashesCoder.flush()
    hashes.toByteArray
  }

  def asPacketsBuffer = {
    hashesCoder.flush()
    pointsCoder.flush()
    new PackedPointsBuffer(points, hashes, pointsCount, cumulativeHash)
  }
}

class ExpandingBuffer(extent: Int) extends OutputStream {
  private[this] var buf = new Array[Byte](extent)
  private[this] var count = 0;

  def write(b: Int) {
    val newcount = count + 1
    if (newcount > buf.length) {
      buf = util.Arrays.copyOf(buf, Math.max(buf.length + extent, newcount))
    }
    buf(count) = b.asInstanceOf[Byte]
    count = newcount
  }

  private[proto] def buffer: Array[Byte] = buf

  override def write(b: Array[Byte], off: Int, len: Int) {
    if ((off < 0) || (off > buf.length) || (len < 0) || ((off + len) > buf.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException
    }
    else if (len == 0) {
      return
    }
    val newcount = count + len
    if (newcount > buf.length) {
      buf = util.Arrays.copyOf(buf, Math.max(buf.length + extent, newcount))
    }
    System.arraycopy(b, off, buf, count, len)
    count = newcount
  }

  def writeTo(out: OutputStream) {
    out.write(buf, 0, count)
  }

  def toByteArray: Array[Byte] = {
    util.Arrays.copyOf(buf, count)
  }

  def toCodedInputStream: CodedInputStream = {
    CodedInputStream.newInstance(buf, 0, count)
  }
}

case class PackedPointsIndex(hash: Int, offset: Int, len: Int, delimitedPoints: Array[Byte], cumulativeHash: Int)

class PackedPointsBuffer private[proto](
                                         points: ExpandingBuffer,
                                         hashes: ExpandingBuffer,
                                         pointsCount: Int,
                                         val cumulativeHash: Int)
  extends Traversable[PackedPointsIndex] {

  override def size: Int = pointsCount

  override def isEmpty: Boolean = {
    pointsCount == 0
  }

  def foreach[U](f: (PackedPointsIndex) => U) {
    val meta = hashes.toCodedInputStream
    val data = points.toCodedInputStream
    var off = 0
    while (!meta.isAtEnd) {
      val hash = meta.readInt32()
      val size = data.readRawVarint32()
      data.skipRawBytes(size) // skip message
      val rawSize = size + CodedOutputStream.computeRawVarint32Size(size)
      f(PackedPointsIndex(hash, off, rawSize, points.buffer, cumulativeHash))
      off += rawSize
    }
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

  def decodeWithBatchId(buffer: Array[Byte]): (Long, Seq[Point]) = {
    val cs = CodedInputStream.newInstance(buffer)
    val batchId = cs.readInt64()
    val points = new ArrayBuffer[Point](buffer.length / expectedMessageSize)
    while (!cs.isAtEnd) {
      val size = cs.readRawVarint32()
      val limit = cs.pushLimit(size)
      points += Point.newBuilder().mergeFrom(cs).build
      cs.popLimit(limit)
    }
    (batchId, points)
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
    val pack = new PackedPoints
    points foreach pack.append
    pack
  }

  @inline
  implicit def asPackedPointsBuffer(points: PackedPoints): PackedPointsBuffer = {
    points.asPacketsBuffer
  }
}
