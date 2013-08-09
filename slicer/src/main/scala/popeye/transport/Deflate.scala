package popeye.transport

import java.util.zip.{DataFormatException, ZipException, Inflater}
import akka.util.ByteString
import scala.annotation.tailrec

/**
 * @author Andrey Stepachev
 */
class Deflate(var limit: Int) {

  var closed = false

  def isClosed = closed

  val inflater = new Inflater()
  private[this] val inputBuf = new Array[Byte](2048)
  private[this] val outputBuf = new Array[Byte](2048)

  def decodeAsSeq(rawInput: ByteString): (Seq[ByteString], Option[ByteString]) = {
    val t = decode(rawInput)
    (t._1.toSeq, t._2)
  }

  /**
   * Stateful decoding
   * @param rawInput input to process
   * @return (uncompressed: Traversable, Some(remainder)), of remainder exists, decoding finished
   */
  def decode(rawInput: ByteString): (TraversableOnce[ByteString], Option[ByteString]) = {
    def traversable(input: ByteString): Traversable[ByteString] = new Traversable[ByteString] {
      def foreach[U](f: (ByteString) => U) {
        limit -= input.length // calling parent should ensure, that limit is obeyed
        decompress(input, f)
        if (limit == 0) {
          closed = true
        }
      }
    }
    if (closed)
      throw new IllegalStateException("Decoder closed")
    if (limit >= rawInput.length) {
      (traversable(rawInput), None)
    } else {
      (traversable(rawInput.take(limit)), Some(rawInput.drop(limit)))
    }
  }

  protected def decompress[U](input: ByteString, f: (ByteString) => U) = {
    input.asByteBuffers foreach {
      buffer =>
        while (buffer.hasRemaining) {
          var off = 0
          do {
            val len = Math.min(inputBuf.length, buffer.remaining())
            buffer.get(inputBuf, off, len)
            off += len
          } while (off < inputBuf.length && buffer.hasRemaining)
          if (off > 0) {
            inflater.setInput(inputBuf, 0, off)
            drain(f)
            if (inflater.needsDictionary) throw new ZipException("ZLIB dictionary missing")
          }
        }
    }
  }

  @tailrec
  private def drain[U](f: (ByteString) => U): Unit = {
    if (!inflater.finished()) {
      val len = inflater.inflate(outputBuf)
      if (len > 0) {
        f(ByteString.fromArray(outputBuf, 0, len))
        drain(f)
      }
    }
  }
}
