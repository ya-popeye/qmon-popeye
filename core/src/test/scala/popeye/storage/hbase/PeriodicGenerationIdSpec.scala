package popeye.storage.hbase

import org.scalatest.{Matchers, FlatSpec}
import org.apache.hadoop.hbase.util.Bytes
import PeriodicGenerationId._
import GenerationIdMapping._

class PeriodicGenerationIdSpec extends FlatSpec with Matchers {
  behavior of "PeriodicTimeRangeId"

  it should "find timerange" in {
    val configs = Seq(
      PeriodConfig(0, 1, 0)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)
    timeRangeIdMapping.backwardIterator(7200).next() should equal(TimeRangeAndId(7200, 10800, 2))
    timeRangeIdMapping.getGenerationId(7200, 7200) should equal(2)
  }

  it should "find timerange in second config" in {
    val configs = Seq(
      PeriodConfig(0, 1, 0),
      PeriodConfig(7200, 2, 2)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)
    timeRangeIdMapping.backwardIterator(7200).next() should equal(TimeRangeAndId(7200, 14400, 2))
  }

  it should "iterate over timeranges" in {
    val configs = Seq(
      PeriodConfig(0, 1, 0),
      PeriodConfig(7200, 2, 2)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)
    val expected = List(
      TimeRangeAndId(14400, 21600, 3),
      TimeRangeAndId(7200, 14400, 2),
      TimeRangeAndId(3600, 7200, 1),
      TimeRangeAndId(0, 3600, 0)
    )
    timeRangeIdMapping.backwardIterator(20000).toList should equal(expected)
  }

  it should "be aware of outlanders" in {
    val configs = Seq(
      PeriodConfig(0, 1, 0)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)

    timeRangeIdMapping.getGenerationId(
      timestampInSeconds = 7200,
      currentTimeInSeconds = 0
    ) should equal(outlanderGenerationId)
  }

  it should "return outlanders id if timestamp is smaller than start time" in {
    val configs = Seq(
      PeriodConfig(3600, 1, 0)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)

    timeRangeIdMapping.getGenerationId(
      timestampInSeconds = 0,
      currentTimeInSeconds = 0
    ) should equal(outlanderGenerationId)
  }

  behavior of "PeriodicTimeRangeId.createPeriodConfigs"

  it should "create simple config" in {
    createPeriodConfigs(Seq(
      (0, 1)
    )) should equal(Seq(PeriodConfig(0, 1, 0)))
  }

  it should "create two configs" in {
    createPeriodConfigs(Seq(
      (0, 1),
      (7200, 10)
    )) should equal(Seq(
      PeriodConfig(0, 1, 0),
      PeriodConfig(7200, 10, 2)
    ))
  }

  it should "create two configs from not aligned data" in {
    createPeriodConfigs(Seq(
      (0, 1),
      (7000, 10)
    )) should equal(Seq(
      PeriodConfig(0, 1, 0),
      PeriodConfig(7200, 10, 2)
    ))
  }

  it should "be aware of outlanders" in {
    createPeriodConfigs(Seq(
      (0, 1),
      (0, 10)
    )) should equal(Seq(
      PeriodConfig(0, 1, 0),
      PeriodConfig(3600 * (outlanderThreshold + 1), 10, 2)
    ))
  }

  it should "handle non-null start times" in {
    val periodConfigs = createPeriodConfigs(Seq(
      (3600, 1),
      (3600 * 3, 2)
    )) should equal(Seq(
      PeriodConfig(3600, 1, 0),
      PeriodConfig(3600 * 3, 2, 2)
    ))
  }
}
