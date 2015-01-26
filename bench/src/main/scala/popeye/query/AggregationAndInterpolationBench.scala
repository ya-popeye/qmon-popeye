package popeye.query

import popeye.storage.PointsSeriesMap
import popeye.{PointRope, Point}
import popeye.bench.BenchUtils
import popeye.storage.PointsGroups

import scala.collection.immutable.SortedMap
import scala.util.Random

object AggregationAndInterpolationBench {
  val timeStep: Int = 300
  val aggregatorKey: String = "avg"
  val samples = 20

  def main(args: Array[String]): Unit = {
    println(f"OpenTSDB2HttpApiServer.aggregatePoints benchmark")
    println(f"time step: $timeStep, aggregator: $aggregatorKey")
    println()

    benchmark(iterations = 100, numberOfSeries = 1, pointsPerSeries = 1000, rate = false)
    benchmark(iterations = 100, numberOfSeries = 10, pointsPerSeries = 1000, rate = false)
    benchmark(numberOfSeries = 100, pointsPerSeries = 1000, rate = false)
    benchmark(numberOfSeries = 100, pointsPerSeries = 1000, rate = true)
  }

  def benchmark(iterations: Int = 1,
                numberOfSeries: Int,
                pointsPerSeries: Int,
                rate: Boolean) = {
    val random = new Random()
    val startTime = 1416395727 // Wed Nov 19 14:15:27 MSK 2014
    val seriesStartTimes = List.fill(numberOfSeries)(random.nextInt(timeStep))
    val serieses = seriesStartTimes.map {
      startTime =>
        val points = (0 until pointsPerSeries).map {
          i =>
            val timestamp = startTime + i * timeStep
            val value = random.nextDouble() * 100 + 200
            Point(timestamp, value)
        }
        PointRope.fromIterator(points.iterator)
    }
    val pointsGroup = serieses.zipWithIndex.map {
      case (series, index) =>
        val seriesTags = SortedMap("index" -> index.toString)
        (seriesTags, series)
    }.toMap
    val pointsGroups = PointsGroups(Map(SortedMap("key" -> "value") -> PointsSeriesMap(pointsGroup)))
    import OpenTSDB2HttpApiServer._
    val aggregator = aggregators(aggregatorKey)
    val benchResult = BenchUtils.bench(samples, iterations) {
      aggregatePoints(
        pointsGroups,
        aggregator,
        rate
      ).toList
    }
    println(f"number of series: $numberOfSeries, points per series: $pointsPerSeries, rate: $rate")
    println(f"min time: ${ benchResult.minTime }, median time: ${ benchResult.medianTime }")
    println()
  }

  def time[T](body: => Unit) = {
    val startTime = System.currentTimeMillis()
    body
    System.currentTimeMillis() - startTime
  }

}
