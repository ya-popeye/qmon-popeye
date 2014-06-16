package popeye.storage.hbase

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
}

// points are packed in rows by hours, so period must be hour-aligned
case class PeriodConfig(startTime: Int, periodInHours: Int, firstPeriodId: Short) {
  def periodInSeconds: Int = periodInHours * 3600

  def getPeriodId(timestamp: Int): Short = {
    require(startTime <= timestamp)
    val delta: Short = ((timestamp - startTime) / periodInSeconds).toShort
    (firstPeriodId + delta).toShort
  }
}

object PeriodicGenerationId {

  import GenerationIdMapping._

  def createPeriodConfigs(configs: Seq[(Int, Int)]) = {
    val startTimeAndPeriods = configs.sortBy { case (time, _) => time }
    val firstConfig = {
      val (startTime, period) = startTimeAndPeriods.head
      PeriodConfig(startTime, period, 0)
    }
    startTimeAndPeriods.tail.scanLeft(firstConfig) {
      case (previousConfig, (startTime, period)) =>
        val earliestPossibleConfigStartTime =
          previousConfig.startTime + previousConfig.periodInSeconds * (outlanderThreshold + 1)
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
        val nextFirstPeriodId = (previousConfig.firstPeriodId + nextStartTime / previousConfig.periodInSeconds).toShort
        PeriodConfig(nextStartTime, period, nextFirstPeriodId)
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
  require(HBaseStorage.UniqueIdGenerationWidth == 2, "PeriodicTimeRangeId depends on generation id width")


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
    val periodStartTime = timestampInSeconds - timestampInSeconds % currentPeriodConfig.periodInSeconds
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
