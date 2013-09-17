package popeye.transport.server

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * @author Andrey Stepachev
 */
class TelnetCommandsSpec extends FlatSpec with ShouldMatchers {
  "Parser" should "handle longs" in {
    val a = Array("978426007", "8")
    a.forall{ l =>
      val parsed = TelnetCommands.parseLong(l)
      val actual = l.toLong
      parsed == actual
    } should be(true)
  }
}
