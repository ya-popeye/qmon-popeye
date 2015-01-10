package popeye.storage.hbase

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.typesafe.config._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.immutable.SortedMap

object GenerationIdMapping {
  val outlanderThreshold = 1
  val outlanderGenerationId: Short = 0xffff.toShort // -1
}

trait GenerationIdMapping {
  def getGenerationId(timestampInSeconds: Int, currentTimeInSeconds: Int): Short

  def backwardIterator(timestampInSeconds: Int): Iterator[TimeRangeAndId]
}

case class TimeRangeAndId(start: Int, stop: Int, id: Short) {
  def contains(timestamp: Int) = start <= timestamp && timestamp < stop

  override def toString: String = f"${ getClass.getSimpleName }($start, $stop, ${ new BytesKey(Bytes.toBytes(id)) }})"
}

// points are packed in rows by timespans, so period must be timespan-aligned
case class PeriodConfig(startTime: Int, periodInTimespans: Int, firstPeriodId: Short) {
  def periodInSeconds: Int = {
    periodInTimespans * TsdbFormat.MAX_TIMESPAN
  }

  def getPeriodId(timestamp: Int): Short = {
    require(startTime <= timestamp)
    val delta: Short = ((timestamp - startTime) / periodInSeconds).toShort
    (firstPeriodId + delta).toShort
  }
}

case class StartTimeAndPeriod(startDateString: String, periodInHours: Int) {
  val startTimeUnixSeconds = {
    val time = StartTimeAndPeriod.dateFormatter.parse(startDateString).getTime
    (time / 1000).toInt
  }
}

object StartTimeAndPeriod {

  import scala.collection.JavaConverters._

  val dateFormatter = {
    val df = new SimpleDateFormat("dd/MM/yy")
    df.setTimeZone(TimeZone.getTimeZone("Etc/UTC"))
    df
  }

  val rotationPeriodKey = "rotation-period-hours"
  val startDateKey = "start-date"

  def renderConfigList(startTimeAndPeriods: Seq[StartTimeAndPeriod]) = {
    startTimeAndPeriods.map {
      case StartTimeAndPeriod(startDateString, period) =>
        ConfigFactory.parseString(
          f"""
            |{
            |  $startDateKey = $startDateString
            |  $rotationPeriodKey = $period
            |}
          """.stripMargin
        )
    }.asJava
  }

  def parseConfigList(configs: java.util.List[_ <: Config]) = {
    configs.asScala.map {
      genConfig =>
        val periodInHours = genConfig.getInt("rotation-period-hours")
        val startDateString = genConfig.getString("start-date")
        StartTimeAndPeriod(startDateString, periodInHours)
    }
  }
}

object PeriodicGenerationId {

  import GenerationIdMapping._

  def createPeriodConfigs(configs: Seq[StartTimeAndPeriod]) = {
    val startTimeAndPeriods = configs.sortBy { case StartTimeAndPeriod(time, _) => time }
    val firstConfig = {
      val startTimeAndPeriod = startTimeAndPeriods.head
      PeriodConfig(startTimeAndPeriod.startTimeUnixSeconds, startTimeAndPeriod.periodInHours, 0)
    }
    startTimeAndPeriods.tail.scanLeft(firstConfig) {
      case (previousConfig, startTimeAndPeriod) =>
        val earliestPossibleConfigStartTime =
          previousConfig.startTime + previousConfig.periodInSeconds * (outlanderThreshold + 1)
        val startTime = startTimeAndPeriod.startTimeUnixSeconds
        val nextStartTime =
          if (earliestPossibleConfigStartTime >= startTime) {
            earliestPossibleConfigStartTime
          } else {
            val remainder = (startTime - previousConfig.startTime) % previousConfig.periodInSeconds
            val lastPreviousConfigPeriodStart =
              if (remainder == 0) {
                startTime - previousConfig.periodInSeconds
              } else {
                startTime - remainder
              }
            lastPreviousConfigPeriodStart + previousConfig.periodInSeconds
          }
        val previousConfigLifespan = nextStartTime - previousConfig.startTime
        val nextFirstPeriodId =
          (previousConfig.firstPeriodId + previousConfigLifespan / previousConfig.periodInSeconds).toShort
        PeriodConfig(nextStartTime, startTimeAndPeriod.periodInHours, nextFirstPeriodId)
    }
  }

  def apply(periodConfigs: Seq[PeriodConfig]) = {
    val startTimes = periodConfigs.map(_.startTime)
    require(startTimes == startTimes.sorted, "start times are not sorted")
    def checkPeriods(periods: List[PeriodConfig]): Unit = periods match {
      case period :: Nil => //ok
      case first :: second :: rest =>
        val lifespan = second.startTime - first.startTime
        require(lifespan % first.periodInSeconds == 0)
        val numberOfTimeRanges = lifespan / first.periodInSeconds
        require(first.firstPeriodId + numberOfTimeRanges == second.firstPeriodId, "range ids do not match")
        checkPeriods(second :: rest)
    }
    checkPeriods(periodConfigs.toList)
    val timeAndConfigs = periodConfigs.map(p => (p.startTime, p))
    new PeriodicGenerationId(SortedMap(timeAndConfigs: _*))
  }
}

class PeriodicGenerationId(periodConfigs: SortedMap[Int, PeriodConfig]) extends GenerationIdMapping {

  val earliestStartTime = periodConfigs.firstKey
  require(TsdbFormat.UniqueIdGenerationWidth == 2, "PeriodicTimeRangeId depends on generation id width")


  override def getGenerationId(timestampInSeconds: Int, currentTimeInSeconds: Int): Short = {
    if (timestampInSeconds < earliestStartTime) {
      GenerationIdMapping.outlanderGenerationId
    } else {
      val timestampPeriodId = getPeriodId(timestampInSeconds)
      val currentPeriodId = getPeriodId(currentTimeInSeconds)
      if (timestampPeriodId - currentPeriodId > GenerationIdMapping.outlanderThreshold) {
        GenerationIdMapping.outlanderGenerationId
      } else {
        timestampPeriodId
      }
    }
  }

  def backwardIterator(timestampInSeconds: Int): Iterator[TimeRangeAndId] = {
    val iteratorPeriods = periodConfigs.to(timestampInSeconds).values
    val currentPeriodConfig = iteratorPeriods.last
    val timeFromConfigStartTime = timestampInSeconds - currentPeriodConfig.startTime
    val periodStartTime = timestampInSeconds - timeFromConfigStartTime % currentPeriodConfig.periodInSeconds
    val periodStopTime = periodStartTime + currentPeriodConfig.periodInSeconds
    new TimeRangesIterator(periodStopTime, iteratorPeriods.toSeq)
  }

  private def getPeriodId(time: Int) = periodConfigs.to(time).last._2.getPeriodId(time)
}

class TimeRangesIterator(var periodStopTime: Int, var periodConfigs: Seq[PeriodConfig]) extends Iterator[TimeRangeAndId] {
  override def hasNext: Boolean = periodConfigs.nonEmpty

  override def next(): TimeRangeAndId = {
    val currentPeriodConfig = periodConfigs.last
    val periodStartTime = periodStopTime - currentPeriodConfig.periodInSeconds
    val id = currentPeriodConfig.getPeriodId(periodStartTime)
    val currentTimeRange = TimeRangeAndId(periodStartTime, periodStopTime, id)

    periodStopTime -= currentPeriodConfig.periodInSeconds
    if (periodStartTime == currentPeriodConfig.startTime) {
      periodConfigs = periodConfigs.init
    }

    currentTimeRange
  }
}
