package popeye.transport

import akka.util.ByteString
import scala.annotation.tailrec

/**
 * @author Andrey Stepachev
 */
class LineDecoder(maxSize: Int = 2048, charset: String = "UTF-8") {

  var buffer: ByteString = ByteString.empty

  /**
   * Try to find delimited string. Delimiters are '\r\n' or '\n'
   * @param input string
   * @return (Some(parsedFragment), Some(reminder))
   */
  def tryParse(input: ByteString): (Option[ByteString], Option[ByteString]) = {
    if (input.length == 0)
      return (None, None)
    val matchPosition = input.indexOf('\n')
    if (matchPosition == -1) {
      (None, Some(cutSlashR(input)))
    } else {
      val remainder = input.drop(matchPosition + 1)
      (Some(
        cutSlashR(input.take(matchPosition))),
        if (remainder.isEmpty) None else Some(remainder))
    }
  }

//  def traverse(input: ByteString): Traversable = new Traversable[ByteString] {
//    def foreach[U](f: (ByteString) => U) {
//      @tailrec
//      def next(i: ByteString): ByteString
//    }
//  }

  @inline
  private[this] def cutSlashR(input: ByteString): ByteString = {
    if (input.last == '\r')
      input.dropRight(1)
    else
      input
  }
}
