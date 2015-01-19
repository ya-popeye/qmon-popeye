package popeye.storage.hbase

import org.scalatest.{Matchers, FlatSpec}
import org.apache.hadoop.hbase.util.Bytes
import PeriodicGenerationId._
import GenerationIdMapping._
import TsdbFormat._

class PeriodicGenerationIdSpec extends FlatSpec with Matchers {
  behavior of "PeriodicTimeRangeId"
  val MTS = MAX_TIMESPAN

  it should "find timerange" in {
    val configs = Seq(
      PeriodConfig(startTime = 0, periodInTimespans = 1, firstPeriodId = 0)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)
    timeRangeIdMapping.backwardIterator(MTS * 2).next() should equal(TimeRangeAndId(MTS * 2, MTS * 3, 2))
    timeRangeIdMapping.getGenerationId(MTS * 2, MTS * 2) should equal(2)
  }

  it should "find timerange in second config" in {
    val configs = Seq(
      PeriodConfig(0, 1, 0),
      PeriodConfig(MTS * 2, 2, 2)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)
    timeRangeIdMapping.backwardIterator(MTS * 2).next() should equal(TimeRangeAndId(MTS * 2, MTS * 4, 2))
  }

  it should "iterate over timeranges" in {
    val configs = Seq(
      PeriodConfig(0, 1, 0),
      PeriodConfig(MTS * 2, 2, 2)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)
    val expected = List(
      TimeRangeAndId(MTS * 4, MTS * 6, 3),
      TimeRangeAndId(MTS * 2, MTS * 4, 2),
      TimeRangeAndId(MTS, MTS * 2, 1),
      TimeRangeAndId(0, MTS, 0)
    )
    timeRangeIdMapping.backwardIterator(MTS * 6 - 1).toList should equal(expected)
  }

  it should "be aware of outlanders" in {
    val configs = Seq(
      PeriodConfig(0, 1, 0)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)

    timeRangeIdMapping.getGenerationId(
      timestampInSeconds = MTS * 2,
      currentTimeInSeconds = 0
    ) should equal(outlanderGenerationId)
  }

  it should "return outlanders id if timestamp is smaller than start time" in {
    val configs = Seq(
      PeriodConfig(MTS, 1, 0)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)

    timeRangeIdMapping.getGenerationId(
      timestampInSeconds = 0,
      currentTimeInSeconds = 0
    ) should equal(outlanderGenerationId)
  }

  it should "handle non-null start times" in {
    val configs = Seq(
      PeriodConfig(MTS, 10, 0)
    )
    val timeRangeIdMapping = PeriodicGenerationId(configs)

    timeRangeIdMapping.backwardIterator(MTS).next().id should equal(0)
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
    val second = StartTimeAndPeriod("29/01/14", 10)
    val startTimeAndPeriods = Seq(first, second)

    createPeriodConfigs(startTimeAndPeriods) should equal(Seq(
      PeriodConfig(first.startTimeUnixSeconds, 1, 0),
      PeriodConfig(second.startTimeUnixSeconds, 10, 2)
    ))
  }

  it should "be aware of outlanders" in {
    val first = StartTimeAndPeriod("01/01/14", 1)
    val second = StartTimeAndPeriod("01/01/14", 2)
    val startTimeAndPeriods = Seq(first, second)
    createPeriodConfigs(startTimeAndPeriods) should equal(Seq(
      PeriodConfig(first.startTimeUnixSeconds, 1, 0),
      PeriodConfig(first.startTimeUnixSeconds + 1 * MTS * (outlanderThreshold + 1), 2, 2)
    ))
  }

  behavior of "StartTimeAndPeriod config parsing and rendering"

  it should "roundtrip" in {
    val startTimeAndPeriods = Seq(
      StartTimeAndPeriod("01/01/14", 1),
      StartTimeAndPeriod("02/01/14", 2),
      StartTimeAndPeriod("03/01/14", 3)
    )
    val configList = StartTimeAndPeriod.renderConfigList(startTimeAndPeriods)
    val result = StartTimeAndPeriod.parseConfigList(configList)
    result should equal(startTimeAndPeriods)
  }

  it should "empty case" in {
    val startTimeAndPeriods = Seq()
    val configList = StartTimeAndPeriod.renderConfigList(startTimeAndPeriods)
    val result = StartTimeAndPeriod.parseConfigList(configList)
    result should equal(startTimeAndPeriods)
  }
}
