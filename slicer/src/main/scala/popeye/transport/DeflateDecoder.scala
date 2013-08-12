package popeye.transport

import java.util.zip.{DataFormatException, ZipException, Inflater}
import akka.util.ByteString
import scala.annotation.tailrec
import java.io.Closeable

/**
 * @author Andrey Stepachev
 */
class DeflateDecoder(var limit: Int) extends Closeable {

  def isClosed = closed
  private[this] var closed = false

  private[this] val inflater = new Inflater()
  private[this] val inputBuf = new Array[Byte](10*1024)
  private[this] val outputBuf = new Array[Byte](64*1024)
  private[this] var pushedBack: Option[ByteString] = None

  /**
   * Return decoded bytes to internal buffer.
   * Subsequent calls of decode will receive that before
   * newly decoded data
   * Doesnt affect the limit
   * @param what what to buffer
   */
  def pushBack(what: ByteString): Unit = {
    pushedBack match {
      case Some(b) => pushedBack = Some(b ++ what)
      case None => pushedBack = Some(what)
    }
  }

  final def decode[U](rawInput: Traversable[ByteString])(f: (ByteString) => U): Option[ByteString] = {
    val b = ByteString.newBuilder
    rawInput.foreach(b.append)
    val input = b.result().compact
    if (input.isEmpty)
      return None
    decode(input)(f)
  }

  /**
   * Stateful decoding
   * @param rawInput input to process
   * @return (uncompressed: Traversable, Some(remainder)), of remainder exists, decoding finished
   */
  def decode[U](rawInput: ByteString)(f: (ByteString) => U): Option[ByteString] = {
    if (isClosed)
      throw new IllegalStateException("Decoder closed")

    def traverse(input: ByteString): Unit = {
      decompress(input, f)
      limit -= input.length // calling parent should ensure, that limit is obeyed
      if (limit == 0) {
        close()
      }
    }

    if (limit >= rawInput.length) {
      traverse(rawInput)
      None
    } else {
      val savedLimit = limit
      traverse(rawInput.take(limit))
      Some(rawInput.drop(savedLimit))
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
        val data = ByteString.fromArray(outputBuf, 0, len)
        val completeData = if (pushedBack.isDefined) {
          val pb = pushedBack.get
          pushedBack = None
          pb ++ data
        } else {
          data
        }
        f(completeData)
        drain(f)
      }
    }
  }

  def close() {
    if (!inflater.finished())
      inflater.end()
    closed = true
  }
}
