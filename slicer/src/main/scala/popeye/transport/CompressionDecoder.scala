package popeye.transport

import java.util.zip.{ZipException, Inflater}
import akka.util.ByteString
import scala.annotation.tailrec
import java.io.Closeable
import CompressionDecoder._

object CompressionDecoder {

  trait Decoder {
    protected var pushedBack: Option[ByteString] = None

    def pushBack(what: ByteString) = {
      pushedBack match {
        case Some(b) => pushedBack = Some(b ++ what)
        case None => pushedBack = Some(what)
      }
    }

    def decompress[U](input: ByteString, f: (ByteString) => U): Unit

    def close(): Unit
  }

  class InflaterDecoder extends Decoder {
    protected val outputBuf = new Array[Byte](64 * 1024)
    private[this] var remaining: Option[ByteString] = None
    private[this] val inflater = new Inflater()

    def decompress[U](input: ByteString, f: (ByteString) => U): Unit = {
      if (input.length > 0) {
        val buffer = if (remaining.isDefined) {
          (remaining.get ++ input).toArray
        } else {
          input.toArray
        }
        inflater.setInput(buffer)
        drain(f)

        if (inflater.getRemaining > 0) {
          remaining = Some(ByteString.fromArray(
            buffer,
            buffer.length - inflater.getRemaining,
            buffer.length)
          )
        }
        validate()
      }
    }

    @tailrec
    private def drain[U](f: (ByteString) => U): Unit = {
      val len = inflater.inflate(outputBuf, 0, outputBuf.length)
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

    def close(): Unit = {
      if (!inflater.finished())
        inflater.end()
    }

    private def validate(): Boolean = if (inflater.needsDictionary)
      throw new ZipException("ZLIB dictionary missing")
    else
      true

  }

  class SnappyDecoder extends Decoder {
    private[this] var buffered = ByteString.empty

    def close(): Unit = {
      buffered = ByteString.empty
    }

    def decompress[U](input: ByteString, f: (ByteString) => U): Unit = {
      throw new NotImplementedError("not yet")
    }
  }


  sealed trait Codec {
    def makeDecoder: Decoder
  }

  case class Gzip() extends Codec {
    def makeDecoder: Decoder = new InflaterDecoder
  }

  case class Snappy() extends Codec {
    def makeDecoder: Decoder = new SnappyDecoder
  }

}

/**
 * @author Andrey Stepachev
 */
class CompressionDecoder(var limit: Int, val codec: Codec = Gzip()) extends Closeable {

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
