package popeye.storage.hbase

import org.scalatest.{Matchers, FlatSpec}

class TsdbFormatConfigSpec extends FlatSpec with Matchers {
  behavior of "TsdbFormatConfig"
  it should "roundtrip" in {
    val config = TsdbFormatConfig(
      startTimeAndPeriods = Seq(
        StartTimeAndPeriod("01/01/14", 24),
        StartTimeAndPeriod("05/01/14", 24)
      ),
      shardAttributes = Set("cluster", "shard")
    )
    val typesafeConfig = TsdbFormatConfig.renderConfig(config)
    TsdbFormatConfig.parseConfig(typesafeConfig) should equal(config)
  }
}
