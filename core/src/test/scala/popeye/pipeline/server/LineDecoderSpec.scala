package popeye.transport.server

import org.scalatest.FlatSpec
import akka.util.ByteString
import popeye.util.LineDecoder

/**
 * @author Andrey Stepachev
 */
class LineDecoderSpec extends FlatSpec {

  val ld = new LineDecoder()
  val a = ByteString("a")
  val b = ByteString("b")
  val r = ByteString("\r")
  val rn = ByteString("\r\n")

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
    ld.tryParse(a ++ b ++ r) match {
      case (None, Some(s)) => assert(s == a ++ b ++ r)
      case x => fail("wrong " + x)
    }
  }

  it should "Whole line" in {
    ld.tryParse(a ++ b ++ ByteString("\r\n")) match {
      case (Some(ab), None) => assert(a ++ b == ab, "got: " + ab)
      case x => fail("wrong " + x)
    }
  }

  it should "Whole line, \\n only" in {
    ld.tryParse(a ++ b ++ ByteString("\n")) match {
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

  "LineDecoder(1)" should "throw" in {
    val ll = new LineDecoder(1)
    intercept[IllegalArgumentException](ll.tryParse(a ++ b))
  }

  "LineDecoder(3)" should "not throw if eol found" in {
    val ll = new LineDecoder(3)
    ll.tryParse(a ++ rn ++ b)
  }

  "LineDecoder" should "split lines" in {
    val samples = List(
      " some line",
      "  some line",
      "some  line",
      "some line ",
      "some line"
    )
    val r = samples.map(l => LineDecoder.split(l, ' ', preserveAllTokens = false)
      .mkString("=")).find(_ != "some=line")
    assert(r)
  }

}
