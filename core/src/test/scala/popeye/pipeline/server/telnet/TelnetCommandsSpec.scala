package popeye.pipeline.server.telnet

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import popeye.pipeline.server.telnet.TelnetCommands

/**
 * @author Andrey Stepachev
 */
class TelnetCommandsSpec extends FlatSpec with ShouldMatchers {
  "Parser" should "handle longs" in {
    val a = Array("978426007", "8")
    a.forall { l =>
      val parsed = TelnetCommands.parseLong(l)
      val actual = l.toLong
      parsed == actual
    } should be(true)
  }
}
