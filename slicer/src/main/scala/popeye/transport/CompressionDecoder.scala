package popeye.transport

import java.util.zip.{InflaterInputStream, DeflaterOutputStream, ZipException, Inflater}
import akka.util.ByteString
import scala.annotation.tailrec
import java.io._
import org.xerial.snappy.{Snappy => SnappyJava}
import CompressionDecoder._
import popeye.transport.CompressionDecoder.Gzip
import scala.Some
import com.google.common.io.ByteStreams
import popeye.transport.snappy.Crc32C

object CompressionDecoder {


  sealed trait Codec {
    def makeDecoder: Decoder

    def makeOutputStream(inner: OutputStream): OutputStream

    def makeInputStream(inner: InputStream): InputStream
  }

  case class Gzip() extends Codec {
    def makeDecoder: Decoder = new InflaterDecoder

    def makeOutputStream(inner: OutputStream): OutputStream = new DeflaterOutputStream(inner)

    def makeInputStream(inner: InputStream): InputStream = new InflaterInputStream(inner)
  }

  case class Snappy() extends Codec {
    def makeDecoder: Decoder = new SnappyDecoder

    def makeOutputStream(inner: OutputStream): OutputStream = new SnappyStreamOutputStream(inner)

    def makeInputStream(inner: InputStream): InputStream = new SnappyStreamInputStream(inner)
  }

}

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

object SnappyDecoder {
  val streamIdentifier = Array[Byte]('s', 'N', 'a', 'P', 'p', 'Y')
  val chunkMax = 1 << 16
  val compressedChunk = 0x00
  val uncompressedChunk = 0x01
  val identifierChunk = 0xff
  val reservedUnskipable = (0x02, 0x80)
  val reservedSkipable = (0x80, 0xff)
  val threshold = 1 - 0.125
}

class SnappyDecoder extends Decoder {

  import SnappyDecoder._

  case class Block(tp: Int, len: Int)

  private[this] var pendingBlock = ByteString.empty
  private[this] var block: Option[Block] = None
  private[this] var streamFound = false

  def close(): Unit = {
  }

  def headerSize = 4

  def needForBlock = {
    block match {
      case Some(b) => b.len - pendingBlock.length
      case None => 0
    }
  }

  def decompress[U](input: ByteString, f: (ByteString) => U): Unit = {
    pendingBlock = doDecompress(pendingBlock ++ input, f)
  }

  @tailrec
  private[this] def doDecompress[U](input: ByteString, f: (ByteString) => U): ByteString = {
    block match {
      case None =>
        if (input.length < 4)
          return input
        val (blockType, len) = blockHeader(input)
        if (!streamFound && blockType != identifierChunk)
          throw new ZipException(s"Stream should start with blocktype $identifierChunk, but found $blockType")
        if (len > chunkMax)
          throw new ZipException(s"Block (type=$blockType) length $len exceeds $chunkMax")
        block = Some(Block(blockType, len))
        doDecompress(input.drop(4), f)

      case Some(Block(blockType, len)) =>
        if (input.length < len)
          return input
        if (!streamFound) {
          if (blockType != identifierChunk || !startsWithHeader(input))
            throw new ZipException(s"Unexpected encoder type: $blockType, header: ${input.take(6)}")
          streamFound = true
        } else {
          if (blockType == compressedChunk || blockType == uncompressedChunk) {
            val pushed = pushedBack
            pushedBack = None
            val chunkLen = len - 4
            if (blockType == compressedChunk) {
              val inBuffer = new Array[Byte](len)
              input.copyToArray(inBuffer)
              val outLen = SnappyJava.uncompressedLength(inBuffer, 4, chunkLen) // skip checksum
              val outBuffer = new Array[Byte](outLen)
              SnappyJava.uncompress(
                inBuffer, 4, chunkLen,
                outBuffer, 0)
              f(pushed.getOrElse(ByteString.empty) ++ ByteString.fromArray(outBuffer))
            } else {
              f(pushed.getOrElse(ByteString.empty) ++ input.slice(4, len))
            }
          } else if (blockType <= reservedSkipable._2 && blockType >= reservedSkipable._1) {
            // ok, skip chunk
          } else if (blockType <= reservedUnskipable._2 && blockType >= reservedUnskipable._1) {
            throw new ZipException(s"Found unskipable chunk type:$blockType")
          }
        }
        block = None
        doDecompress(input.drop(len), f)
    }
  }

  private def startsWithHeader(buf: ByteString): Boolean = {
    buf(0) == streamIdentifier(0) &&
      buf(1) == streamIdentifier(1) &&
      buf(2) == streamIdentifier(2) &&
      buf(3) == streamIdentifier(3) &&
      buf(4) == streamIdentifier(4) &&
      buf(5) == streamIdentifier(5)
  }

  private def blockHeader(buf: ByteString): (Int, Int) = {
    (buf(0) & 0xff,
      ((buf(3) & 0xff) << 16) + ((buf(2) & 0xff) << 8) + (buf(1) & 0xff))
  }

}

class SnappyStreamInputStream(inner: InputStream) extends InputStream {

  import SnappyDecoder._

  private[this] val decoder = new SnappyDecoder()
  private[this] val buffer = new Array[Byte](chunkMax + decoder.headerSize)
  private[this] var consumed = 0
  private[this] var cached = ByteString.empty

  def nextBlock(): Int = {
    if (inner.available() == 0)
      return -1
    val headerSize = decoder.headerSize
    ByteStreams.readFully(inner, buffer, 0, headerSize)
    val chunkSize = inner.read(buffer, headerSize, buffer.length - headerSize)
    consumed = 0
    decoder.decompress(ByteString.fromArray(buffer, 0, chunkSize + headerSize), {
      buf =>
        cached ++= buf
    })
    cached.length
  }

  def read(): Int = {
    if (consumed >= cached.length && nextBlock() == -1)
      return -1
    val off = consumed
    consumed += 1
    cached(off)
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (consumed >= cached.length && nextBlock() == -1)
      return -1
    val forConsume = Math.min(len, cached.length - consumed)
    cached.copyToArray(b, off, forConsume)
    consumed += forConsume
    forConsume
  }
}

class SnappyStreamOutputStream(inner: OutputStream) extends OutputStream {

  import SnappyDecoder._

  writeBlockHeader(identifierChunk, streamIdentifier.length)
  inner.write(streamIdentifier)

  private def writeBlockHeader(blockType: Int, len: Int) {
    inner.write(blockType & 0xFF)
    inner.write(len & 0xFF)
    inner.write((len >> 8) & 0xFF)
    inner.write((len >> 16) & 0xFF)
  }

  private def writeCrc(crc: Int) {
    inner.write(crc & 0xFF)
    inner.write((crc >> 8) & 0xFF)
    inner.write((crc >> 16) & 0xFF)
    inner.write((crc >> 32) & 0xFF)
  }

  private[this] val oneByte = new Array[Byte](1)

  def write(b: Int) {
    oneByte(0) = (b & 0xff).toByte
    write(oneByte, 0, 1)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    writeBlocks(b, off, len)
  }

  @tailrec
  private def writeBlocks(b: Array[Byte], off: Int, len: Int): Unit = {
    val actualLen = Math.min(chunkMax, len)
    val arr = new Array[Byte](SnappyJava.maxCompressedLength(actualLen))
    val compressed = SnappyJava.compress(b, off, actualLen, arr, 0)
    if (compressed < actualLen * threshold) {
      writeBlockHeader(compressedChunk, compressed + 4)
      writeCrc(Crc32C.maskedCrc32c(b, off, actualLen))
      inner.write(arr, 0, compressed)
    } else {
      writeBlockHeader(uncompressedChunk, actualLen + 4)
      writeCrc(Crc32C.maskedCrc32c(b, off, actualLen))
      inner.write(b, off, actualLen)
    }
    if (actualLen < len)
      writeBlocks(b, off + actualLen, len - actualLen)
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
