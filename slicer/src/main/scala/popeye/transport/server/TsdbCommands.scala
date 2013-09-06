package popeye.transport.server

import com.typesafe.config.Config
import java.io.Closeable
import popeye.Logging
import popeye.transport.{LineDecoder, CompressionDecoder}
import akka.util.ByteString
import popeye.transport.proto.Message
import scala.annotation.tailrec
import popeye.transport.CompressionDecoder.{Snappy, Gzip}
import popeye.transport.proto.Message.{Attribute, Point}
import scala.collection.mutable

/**
 * @author Andrey Stepachev
 */
abstract class TsdbCommands(metrics: TsdbTelnetMetrics, config: Config) extends Closeable with Logging {

  private var deflater: Option[CompressionDecoder] = None
  private val lineDecoder = new LineDecoder()
  private var bufferedLine: Option[ByteString] = None

  def addPoint(point: Message.Point): Unit

  def commit(correlationId: Option[Long]): Unit

  def startExit(): Unit

  override def close() = {
    deflater foreach {
      _.close
    }
  }

  def process(data: ByteString) = {
    val concat = if (bufferedLine.isDefined) {
      bufferedLine.get ++ data
    } else {
      data
    }
    bufferedLine = doCommands(concat) map {
      _.compact
    }

  }

  @tailrec
  private def tryParseCommand(input: ByteString): Option[ByteString] = {
    lineDecoder.tryParse(input) match {
      case (None, remainder) =>
        remainder
      case (Some(line), remainder) =>
        val strings = LineDecoder.split(line.utf8String, ' ', preserveAllTokens = false)
        strings(0) match {

          case "deflate" =>
            if (deflater.isDefined)
              throw new IllegalArgumentException("Already in deflate mode")
            deflater = Some(new CompressionDecoder(strings(1).toInt, Gzip()))
            debug(s"Entering deflate mode, expected ${strings(1)} bytes")
            return remainder // early exit, we need to reenter doCommands

          case "snappy" =>
            if (deflater.isDefined)
              throw new IllegalArgumentException("Already in deflate mode")
            deflater = Some(new CompressionDecoder(strings(1).toInt, Snappy()))
            debug(s"Entering snappy mode, expected ${strings(1)} bytes")
            return remainder // early exit, we need to reenter doCommands

          case "put" =>
            metrics.pointsRcvMeter.mark()
            addPoint(parsePoint(strings))

          case "commit" =>
            commit(Some(strings(1).toLong))

          case "version" =>
            commit(None)

          case "exit" =>
            commit(None)
            startExit()

          case c: String =>
            throw new IllegalArgumentException(s"Unknown command ${c.take(50)}")
        }
        remainder match {
          case Some(l) => tryParseCommand(l)
          case None =>
            None
        }
    }
  }

  @tailrec
  private def doCommands(input: ByteString): Option[ByteString] = {

    deflater match {
      case Some(decoder) =>
        val remainder = decoder.decode(input) {
          buf =>
            val parserRemainder = tryParseCommand(buf)
            parserRemainder foreach decoder.pushBack
        }
        if (decoder.isClosed) {
          deflater = None
          debug(s"Leaving encoded ${decoder.codec} mode")
          if (remainder.isDefined)
            doCommands(remainder.get)
          else
            None
        } else {
          None
        }

      case None =>
        val parserRemainder = tryParseCommand(input)
        // we should ensure, that next iteration will no be empty,
        // otherwise we need to return remainder to calling context
        // expected to be buffered somewhere and refeed on next call
        // if decoder activated, reenter doCommands too
        if (deflater.isDefined && parserRemainder.isDefined) {
          doCommands(parserRemainder.get)
        } else {
          parserRemainder
        }
    }
  }

  /**
   * Parses an integer value as a long from the given character sequence.
   * <p>
   * This is equivalent to {@link Long#parseLong(String)} except it's up to
   * 100% faster on {@link String} and always works in O(1) space even with
   * {@link StringBuilder} buffers (where it's 2x to 5x faster).
   * @param s The character sequence containing the integer value to parse.
   * @return The value parsed.
   * @throws NumberFormatException if the value is malformed or overflows.
   */
  def parseLong(s: CharSequence): Long = {
    val n: Int = s.length
    if (n == 0) {
      throw new NumberFormatException("Empty string")
    }
    var c: Char = s.charAt(0)
    var i: Int = 1
    if (c < '0' && (c == '+' || c == '-')) {
      if (n == 1) {
        throw new NumberFormatException("Just a sign, no value: " + s)
      } else if (n > 20) {
        throw new NumberFormatException("Value too long: " + s)
      }
      c = s.charAt(1)
      i = 2
    }
    else if (n > 19) {
      throw new NumberFormatException("Value too long: " + s)
    }
    var v: Long = 0
    do {
      if ('0' <= c && c <= '9') {
        v -= c - '0'
      } else {
        throw new NumberFormatException("Invalid character '" + c + "' in " + s)
      }
      v *= 10
      c = s.charAt(i)
      i += 1
    } while (i < n)
    if (v > 0) {
      throw new NumberFormatException("Overflow in " + s)
    } else if (s.charAt(0) == '-') {
      v
    } else if (v == Long.MinValue) {
      throw new NumberFormatException("Overflow in " + s)
    } else {
      -v
    }
  }

  /**
   * Returns true if the given string looks like an integer.
   * <p>
   * This function doesn't do any checking on the string other than looking
   * for some characters that are generally found in floating point values
   * such as '.' or 'e'.
   * @since 1.1
   */
  def looksLikeInteger(value: String): Boolean = {
    val n: Int = value.length
    var i: Int = 0
    while (i < n) {
      val c: Char = value.charAt(i)
      if (c == '.' || c == 'e' || c == 'E') {
        return false
      }
      i += 1
    }
    true
  }

  def parsePoint(words: Array[String]): Point = {
    val ev = Point.newBuilder()
    words(0) = null; // Ditch the "put".
    if (words.length < 5) {
      // Need at least: metric timestamp value tag
      //               ^ 5 and not 4 because words[0] is "put".
      throw new IllegalArgumentException("not enough arguments"
        + " (need least 4, got " + (words.length - 1) + ')');
    }
    ev.setMetric(words(1));
    if (ev.getMetric.isEmpty) {
      throw new IllegalArgumentException("empty metric name");
    }
    ev.setTimestamp(parseLong(words(2)));
    if (ev.getTimestamp <= 0) {
      throw new IllegalArgumentException("invalid timestamp: " + ev.getTimestamp);
    }
    val value = words(3);
    if (value.length() <= 0) {
      throw new IllegalArgumentException("empty value");
    }
    if (looksLikeInteger(value)) {
      ev.setIntValue(parseLong(value));
    } else {
      // floating point value
      ev.setFloatValue(java.lang.Float.parseFloat(value));
    }
    parseTags(ev, 4, words)
    ev.build
  }

  /**
   * Parses tags into a Point.Attribute structure.
   * @param tags String array of the form "tag=value".
   * @throws IllegalArgumentException if the tag is malformed.
   * @throws IllegalArgumentException if the tag was already in tags with a
   *                                  different value.
   */
  def parseTags(builder: Point.Builder, startIdx: Int, tags: Array[String]) {
    val set = mutable.HashSet[String]()
    for (i <- startIdx until tags.length) {
      val tag = tags(i)
      if (!tag.isEmpty) {
        val kv: Array[String] = LineDecoder.split(tag, '=', preserveAllTokens = true)
        if (kv.length != 2 || kv(0).length <= 0 || kv(1).length <= 0) {
          throw new IllegalArgumentException("invalid tag: " + tag)
        }
        if (!set.add(kv(0))) {
          throw new IllegalArgumentException("duplicate tag: " + tag + ", tags=" + tag)
        }
        builder.addAttributes(Attribute.newBuilder()
          .setName(kv(0))
          .setValue(kv(1)))
      }
    }
  }
}
