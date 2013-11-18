package popeye.proto

import java.io.OutputStream
import java.util
import com.google.protobuf.CodedInputStream

class ExpandingBuffer(extent: Int) extends OutputStream  {

  private val array = new ExpandingArray[Byte](extent)

  def buffer = array.buffer
  def length = array.length
  def offset = array.offset

  def consume(bytesToConsume: Int) = array.consume(bytesToConsume)

  def write(b: Int) {
    array.add(b.asInstanceOf[Byte])
  }

  def write(other: ExpandingBuffer) {
    array.add(other.array)
  }

  override def write(b: Array[Byte], off: Int, len: Int) {
    if ((off < 0) || (len < 0) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException
    }
    else if (len == 0) {
      return
    }
    array.add(b, off, len)
  }

  def writeTo(out: OutputStream) {
    out.write(array.buffer, array.offset, array.length)
  }

  def toByteArray: Array[Byte] = {
    util.Arrays.copyOfRange(array.buffer, array.offset, array.length)
  }

  def toCodedInputStream: CodedInputStream = {
    CodedInputStream.newInstance(array.buffer, array.offset, array.length)
  }
}
