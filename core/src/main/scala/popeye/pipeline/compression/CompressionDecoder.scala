package popeye.pipeline.compression

import CompressionDecoder._
import akka.util.ByteString
import java.io._
import java.util.zip._
import scala.Some
import scala.annotation.tailrec
import popeye.pipeline.compression.CompressionDecoder.Deflate
import scala.Some

object CompressionDecoder {

  sealed trait Codec {
    def makeDecoder: Decoder

    def makeOutputStream(inner: OutputStream): OutputStream

    def makeInputStream(inner: InputStream): InputStream
  }

  class BaseDeflate(noWrap: Boolean = false) extends Codec {
    def makeDecoder: Decoder = new InflaterDecoder(noWrap = noWrap)

    def makeOutputStream(inner: OutputStream): OutputStream = new DeflaterOutputStream(inner,
      new Deflater(Deflater.DEFAULT_COMPRESSION, noWrap))

    def makeInputStream(inner: InputStream): InputStream = new InflaterInputStream(inner,
      new Inflater(noWrap))
  }

  case class Deflate() extends BaseDeflate(noWrap = false)

  case class Gzip() extends BaseDeflate(noWrap = true)

  case class Snappy() extends Codec {
    def makeDecoder: Decoder = new SnappyDecoder

    def makeOutputStream(inner: OutputStream): OutputStream = new SnappyStreamOutputStream(new DataOutputStream(inner))

    def makeInputStream(inner: InputStream): InputStream = new SnappyStreamInputStream(new DataInputStream(inner))
  }

}

/**
 * @author Andrey Stepachev
 */
class CompressionDecoder(var limit: Int, val codec: Codec = Deflate()) extends Closeable {

  def isClosed = closed

  private[this] var closed = false
  private[this] val decoder = codec.makeDecoder

  /**
   * Return decoded bytes to internal buffer.
   * Subsequent calls of decode will receive that before
   * newly decoded data
   * Doesnt affect the limit
   * @param what what to buffer
   */
  def pushBack(what: ByteString): Unit = {
    decoder.pushBack(what)
  }

  @tailrec
  final def decode[U](rawInput: Traversable[ByteString])(f: (ByteString) => U): Option[ByteString] = {
    rawInput.headOption match {
      case Some(head) =>
        val rem = decode(head)(f)
        if (rem.isDefined) {
          // if we got remainder, we finished decoding
          // (due of limit or other causes)
          // need to return all remaining input as remainder
          // thats it, we construct one big ByteString
          require(isClosed)
          val b = ByteString.newBuilder
          b.append(rem.get)
          rawInput.drop(1).foreach(b.append)
          Some(b.result())
        } else if (isClosed) {
          val b = ByteString.newBuilder
          rawInput.drop(1).foreach(b.append)
          Some(b.result())
        } else {
          decode(rawInput.drop(1))(f)
        }
      case None =>
        None
    }

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
      decoder.decompress(input, f)
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

  def close() = {
    if (!closed)
      decoder.close()
    closed = true
  }


}
