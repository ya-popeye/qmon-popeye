package popeye.pipeline.server.telnet

import akka.util.ByteString
import java.io.{ByteArrayInputStream, Closeable}
import popeye.Logging
import popeye.pipeline.compression.CompressionDecoder
import popeye.pipeline.compression.CompressionDecoder.{Gzip, Snappy, Deflate}
import popeye.proto.Message.{Attribute, Point}
import popeye.proto.{PackedPoints, ExpandingBuffer, Message}
import popeye.util.LineDecoder
import scala.annotation.tailrec
import scala.collection.mutable
import java.util.zip.{Inflater, Deflater}

// TODO: rewrite to FSM
trait CommandListener {
  def addPoint(point: Message.Point): Unit

  def commit(correlationId: Option[Long]): Unit

  def startExit(): Unit
}

class TelnetCommands(metrics: TelnetPointsMetrics,
                     commandListener: CommandListener,
                     shardAttributes: Set[String]) extends Closeable with Logging {

  import TelnetCommands._

  private var deflater: Option[CompressionDecoder] = None
  private val lineDecoder = new LineDecoder()
  private var bufferedLine: Option[ByteString] = None
  private var errorCounts = mutable.HashMap[String, Int]()

  private def incrementErrorCount(errorType: String): Unit = {
    val previousErrorCount = errorCounts.getOrElse(errorType, 0)
    errorCounts(errorType) = previousErrorCount + 1
  }

  def getErrorCounts() = {
    val counts = errorCounts
    errorCounts = mutable.HashMap[String, Int]()
    counts
  }

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
            deflater = Some(new CompressionDecoder(strings(1).toInt, Deflate()))
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
            try {
              import scala.collection.JavaConverters._
              val point = parsePoint(strings)
              if (point.getAttributesList.asScala.count(attr => shardAttributes.contains(attr.getName)) == 1) {
                commandListener.addPoint(point)
              } else {
                incrementErrorCount(s"point have no or more than one shard attribute; " +
                  s"shard attribute names: $shardAttributes")
              }
            } catch {
              case iae@(_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
                incrementErrorCount(iae.getMessage)
                debugThrowable(s"Illegal point: ${strings.mkString(",")}", iae)
              case x: Throwable =>
                throw x
            }

          case "commit" =>
            commandListener.commit(Some(strings(1).toLong))

          case "version" =>
            commandListener.commit(None)

          case "exit" =>
            commandListener.commit(None)
            commandListener.startExit()

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


}

object TelnetCommands {
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
    if (i < s.length()) {
      do {
        if ('0' <= c && c <= '9') {
          v -= c - '0'
        } else {
          throw new NumberFormatException("Invalid character '" + c + "' in " + s)
        }
        v *= 10
        c = s.charAt(i)
        i += 1
      } while (i < s.length())
    }
    v -= c - '0'
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

  def parsePoint(words: mutable.IndexedSeq[String]): Point = {
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
    // list value
    if (value(0) == '[') {
      if (value.last != ']') {
        throw new IllegalArgumentException("bad list value: closing bracket")
      }
      val valueStrings = LineDecoder.split(value.substring(1, value.length - 1), ',', preserveAllTokens = true)
      parseListValue(ev, valueStrings)
    } else if (looksLikeInteger(value)) {
      ev.setIntValue(parseLong(value));
    } else {
      // floating point value
      ev.setFloatValue(java.lang.Float.parseFloat(value));
    }
    parseTags(ev, 4, words)
    ev.build
  }

  def parseListValue(builder: Point.Builder, valueStrings: Iterable[String]) = {
    import scala.collection.JavaConverters._
    val iter = valueStrings.iterator
    if (!iter.hasNext) {
      throw new IllegalArgumentException("empty values list")
    }
    var isFloatList = false
    for (str <- iter) {
      if (!isFloatList && !looksLikeInteger(str)) {
        isFloatList = true
        val intValues = builder.getIntListValueList
        for (intValue <- intValues.asScala) {
          builder.addFloatListValue(intValue.floatValue())
        }
        builder.clearIntListValue()
      }
      if (isFloatList) {
        builder.addFloatListValue(java.lang.Float.parseFloat(str))
      } else {
        builder.addIntListValue(parseLong(str))
      }
    }
  }

  /**
   * Parses tags into a Point.Attribute structure.
   * @param tags String array of the form "tag=value".
   * @throws IllegalArgumentException if the tag is malformed.
   * @throws IllegalArgumentException if the tag was already in tags with a
   *                                  different value.
   */
  def parseTags(builder: Point.Builder, startIdx: Int, tags: mutable.IndexedSeq[String]) {
    val set = mutable.HashSet[String]()
    for (i <- startIdx until tags.length) {
      val tag = tags(i)
      if (!tag.isEmpty) {
        val kv = LineDecoder.split(tag, '=', preserveAllTokens = true)
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
