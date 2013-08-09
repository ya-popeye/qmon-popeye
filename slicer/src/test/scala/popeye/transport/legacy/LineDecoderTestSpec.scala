package popeye.transport.legacy

import org.scalatest.FlatSpec
import popeye.transport.LineDecoder
import akka.util.ByteString

/**
 * @author Andrey Stepachev
 */
class LineDecoderTestSpec extends FlatSpec {

  val ld = new LineDecoder()
  val a = ByteString("a")
  val b = ByteString("b")

  behavior of "LineDecoder"

  it should "Empty line" in {
    ld.tryParse(a) match {
      case (None, Some(s)) =>
      case x => fail("wrong " + x)
    }
  }

  it should "Concatenated line" in {
    ld.tryParse(a ++ b) match {
      case (None, Some(s)) => assert(s == a ++ b)
      case x => fail("wrong " + x)
    }
  }

  it should "Dangling \\r" in {
    ld.tryParse(a ++ b ++ ByteString("\r")) match {
      case (None, Some(s)) =>  assert(s == a ++ b)
      case x => fail("wrong " + x)
    }
  }

  it should "Whole line" in {
    ld.tryParse(a ++ b ++ ByteString("\r\n")) match {
      case (Some(ab), None) => assert(a ++ b == ab, "got: " + ab)
      case x => fail("wrong " + x)
    }
  }

  it should "Reminder" in {
    ld.tryParse(a ++ b ++ ByteString("\r\n") ++ a) match {
      case (Some(ab), Some(s)) => assert(a ++ b == ab && s == a)
      case x => fail("wrong " + x)
    }
  }
}
