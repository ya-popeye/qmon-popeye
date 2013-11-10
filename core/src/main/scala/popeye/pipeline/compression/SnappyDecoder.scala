package popeye.transport.compression

import akka.util.ByteString
import scala.annotation.tailrec
import java.util.zip.ZipException
import org.xerial.snappy.Snappy
import org.xerial.snappy.{Snappy => SnappyJava}
import popeye.pipeline.snappy.Crc32C



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
