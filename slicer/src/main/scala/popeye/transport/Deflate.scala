package popeye.transport

import java.util.zip.{ZipException, Inflater}
import akka.util.ByteString

/**
 * @author Andrey Stepachev
 */
class Deflate(var limit: Int) {

  var closed = false
  def isClosed = closed
  val decompress = new Inflater()
  val tmpBuffer = new Array[Byte](2048)

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
    if (closed)
      throw new IllegalStateException("Decoder closed")
    def traversable(input: ByteString): Traversable[ByteString] = new Traversable[ByteString] {
      def foreach[U](f: (ByteString) => U) {
        input.asByteBuffers foreach {
          buffer =>
            while(buffer.hasRemaining) {
              val len = Math.min(tmpBuffer.length, buffer.remaining())
              buffer.get(tmpBuffer, 0, len)
              val actual: Int = decompress.inflate(tmpBuffer, 0, len)
              if (decompress.needsDictionary()) throw new ZipException("Need ZIP dictionary")
              if (actual > 0)
                f(ByteString.fromArray(tmpBuffer, 0, actual))

              limit -= len
            }
        }
        if (limit == 0) {
          closed = true
          decompress.end()
        }
      }
    }
    if (limit > rawInput.length) {
      (traversable(rawInput), None)
    } else {
      (traversable(rawInput.take(limit)), Some(rawInput.drop(limit)))
    }
  }
}
