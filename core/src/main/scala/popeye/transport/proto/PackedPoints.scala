package popeye.transport.proto

import popeye.transport.proto.Message.Point
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import java.io.{InputStream, ByteArrayOutputStream}
import scala.collection.mutable.ArrayBuffer

/**
 * @author Andrey Stepachev
 */
class PackedPoints {
  private[this] val hashes = new ByteArrayOutputStream()
  private[this] val hashesCoder = CodedOutputStream.newInstance(hashes)
  private[this] val points = new ByteArrayOutputStream()
  private[this] val pointsCoder = CodedOutputStream.newInstance(points)
  var pointsCount = 0

  @inline
  def +=(point: Point) = append(point)

  def append(point: Point) = {
    val hash: Int = point.getMetric.hashCode
    hashesCoder.writeInt32NoTag(hash)
    val size = point.getSerializedSize
    pointsCoder.writeRawVarint32(size)
    point.writeTo(pointsCoder)
    pointsCount += 1
    this
  }

  def pointsBuffer = {
    pointsCoder.flush()
    points.toByteArray
  }

  def hashesBuffer = {
    hashesCoder.flush()
    hashes.toByteArray
  }

  def asPacketsBuffer = {
    hashesCoder.flush()
    pointsCoder.flush()
    new PackedPointsBuffer(points.toByteArray, hashes.toByteArray, pointsCount)
  }
}

case class PackedPointsIndex(hash: Int, offset: Int, len: Int, delimitedPoints: Array[Byte])

class PackedPointsBuffer(points: Array[Byte], hashes: Array[Byte], pointsCount: Int) extends Traversable[PackedPointsIndex] {

  def foreach[U](f: (PackedPointsIndex) => U) {
    val meta = CodedInputStream.newInstance(hashes)
    val data = CodedInputStream.newInstance(points)
    var off = 0
    while (!meta.isAtEnd) {
      val hash = meta.readInt32()
      val size = data.readRawVarint32()
      data.skipRawBytes(size) // skip message
      val rawSize = size + CodedOutputStream.computeRawVarint32Size(size)
      f(PackedPointsIndex(hash, off, rawSize, points))
      off += rawSize
    }
  }
}

object PackedPoints {

  val expectedMessageSize = 50

  def prependBatchId(batchId: Long, two: Array[Byte]) = {
    val b = new ByteArrayOutputStream()
    val cs = CodedOutputStream.newInstance(b)
    cs.writeInt64NoTag(batchId)
    cs.flush()
    val combined = new Array[Byte](b.size + two.length)
    val one = b.toByteArray
    System.arraycopy(one,0,combined,0         ,one.length)
    System.arraycopy(two,0,combined,one.length,two.length)
    combined
  }

  def decodeWithBatchId(buffer: Array[Byte]): (Long, Seq[Point]) = {
    val cs = CodedInputStream.newInstance(buffer)
    val batchId = cs.readInt64()
    val points = new ArrayBuffer[Point](buffer.length / expectedMessageSize)
    while(!cs.isAtEnd) {
      points += Point.newBuilder().mergeFrom(cs).build
    }
    (batchId, points)
  }

  @inline
  def apply(): PackedPoints = {
    new PackedPoints
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
