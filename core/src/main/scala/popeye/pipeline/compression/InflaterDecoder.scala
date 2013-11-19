package popeye.pipeline.compression

import akka.util.ByteString
import java.util.zip.{ZipException, Inflater}
import scala.annotation.tailrec

/**
 * @author Andrey Stepachev
 */
class InflaterDecoder(val noWrap: Boolean = false) extends Decoder {
  protected val outputBuf = new Array[Byte](64 * 1024)
  private[this] var remaining: Option[ByteString] = None
  //   private[this] var remaining: Option[ByteString] = if (noWrap) {
  //     Some(ByteString.apply(new Array[Byte](0)))
  //   } else {
  //     None
  //   }
  private[this] val inflater = new Inflater(noWrap)

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
