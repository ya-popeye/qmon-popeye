
package popeye

import org.scalatest.{Matchers, FlatSpec}
import com.typesafe.config.{ConfigValue, ConfigObject, ConfigFactory}
import java.io.StringReader


class ConfigUtilSpec extends FlatSpec with Matchers {
  behavior of "ConfigUtil.asMap"

  it should "transform config to Map" in {
    val config = parseConfig(
      """
        |conf {
        |  a = {
        |    aa = 1
        |  }
        |  b = {
        |    bb = 2
        |  }
        |}
      """.stripMargin)
    val map = ConfigUtil.asMap(config.getConfig("conf"))
    map("a").getInt("aa") should equal(1)
    map("b").getInt("bb") should equal(2)
  }

  def toConfig(configValue: ConfigValue) = configValue.asInstanceOf[ConfigObject].toConfig

  def parseConfig(string: String) = ConfigFactory.parseReader(new StringReader(string))
}
