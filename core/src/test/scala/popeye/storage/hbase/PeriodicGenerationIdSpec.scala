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

  it should "handle non-null start times" in {
    val configs = Seq(
      PeriodConfig(3600, 10, 0)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)

    timeRangeIdMapping.backwardIterator(3600).next().id should equal(0)
  }

  behavior of "PeriodicTimeRangeId.createPeriodConfigs"

  it should "create simple config" in {
    val startTimeAndPeriod = StartTimeAndPeriod("01/01/14", 1)
    createPeriodConfigs(Seq(
      startTimeAndPeriod
    )) should equal(Seq(PeriodConfig(startTimeAndPeriod.startTimeUnixSeconds, 1, 0)))
  }

  it should "create two configs" in {
    val first = StartTimeAndPeriod("01/01/14", 1)
    val second = StartTimeAndPeriod("02/01/14", 10)
    val startTimeAndPeriods = Seq(first, second)

    createPeriodConfigs(startTimeAndPeriods) should equal(Seq(
      PeriodConfig(first.startTimeUnixSeconds, 1, 0),
      PeriodConfig(second.startTimeUnixSeconds, 10, 24)
    ))
  }

  it should "be aware of outlanders" in {
    val first = StartTimeAndPeriod("01/01/14", 24)
    val second = StartTimeAndPeriod("01/01/14", 48)
    val startTimeAndPeriods = Seq(first, second)
    createPeriodConfigs(startTimeAndPeriods) should equal(Seq(
      PeriodConfig(first.startTimeUnixSeconds, 24, 0),
      PeriodConfig(first.startTimeUnixSeconds + 24 * 3600 * (outlanderThreshold + 1), 48, 2)
    ))
  }

  behavior of "StartTimeAndPeriod config parsing and rendering"

  it should "roundtrip" in {
    val startTimeAndPeriods = Seq(
      StartTimeAndPeriod("01/01/14", 1),
      StartTimeAndPeriod("02/01/14", 2),
      StartTimeAndPeriod("03/01/14", 3)
    )
    val configList = StartTimeAndPeriod.toConfigList(startTimeAndPeriods)
    val result = StartTimeAndPeriod.fromConfigList(configList)
    result should equal(startTimeAndPeriods)
  }

  it should "empty case" in {
    val startTimeAndPeriods = Seq()
    val configList = StartTimeAndPeriod.toConfigList(startTimeAndPeriods)
    val result = StartTimeAndPeriod.fromConfigList(configList)
    result should equal(startTimeAndPeriods)
  }
}
