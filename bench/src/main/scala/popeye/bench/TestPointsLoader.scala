package popeye.bench

import popeye.Logging
import popeye.clients.{TsPoint, SlicerClient}
import popeye.inttesting.TestDataUtils

object TestPointsLoader extends Logging {

  case class Args(slicerHost: String, slicerPort: Int, currentTime: Int, metricName: String, nCopies: Int)

  case class SinSeries(name: String, period: Int, amp: Double)

  val parser = new scopt.OptionParser[Args]("popeye-run-class.sh popeye.bench.TestPointsLoader") {
    head("popeye load test tool", "0.1")
    opt[String]("host") valueName "<slicer host>" action {
      (param, config) =>
        config.copy(slicerHost = param)
    }
    opt[Int]("port") valueName "<port>" action {
      (param, config) => config.copy(slicerPort = param)
    }
    opt[Int]("current_time") required() valueName "<current time>" action {
      (param, config) => config.copy(currentTime = param)
    }
    opt[String]("metric_name") required() valueName "<metric name>" action {
      (param, config) => config.copy(metricName = param)
    }
    opt[Int]("n_copies") valueName "<number of copies>" action {
      (param, config) => config.copy(nCopies = param)
    }
  }

  def main(args: Array[String]) {
    val parsedArgs = parser.parse(args, Args("localhost", 4444, 0, "sin", 1)).get
    val slicerClient = SlicerClient.createClient(parsedArgs.slicerHost, parsedArgs.slicerPort)
    val periods = Seq(1000, 2000, 3000)
    val shifts = Seq(500, 1000, 1500)
    val amps = Seq(10, 20, 30)
    val currentTimeSeconds = parsedArgs.currentTime
    val startTime = currentTimeSeconds - 60 * 60 * 24 * 7 * 4 * 2
    val timestamps = startTime until currentTimeSeconds by 60
    val series = Seq(
      SinSeries("10m", 60 * 10, 100),
      SinSeries("1h", 60 * 60, 100 * math.pow(3, 1)),
      SinSeries("24h", 60 * 60 * 24, 100 * math.pow(3, 2)),
      SinSeries("1w", 60 * 60 * 24 * 7, 100 * math.pow(3, 3)),
      SinSeries("2mon", 60 * 60 * 24 * 7 * 8, 100 * math.pow(3, 4))
    )
    val points = createPoints(parsedArgs.metricName, timestamps, parsedArgs.nCopies, series)
    val totalPointsCount = timestamps.size * parsedArgs.nCopies * series.size
    info(f"total points count: $totalPointsCount")
    var totalSentPoints = 0
    for (group <- points.grouped(1000000)) {
      val progressPercents = (totalSentPoints.toDouble / totalPointsCount * 100).toInt
      info(f"sending ${group.size} points, progress: $progressPercents ${"%"}")
      slicerClient.putPoints(group)
      totalSentPoints += group.size
      if (slicerClient.commit()) {
        info("commit succeeded")
      } else {
        info("commit failed")
      }
    }
  }

  def createPoints(metricName: String, timestamps: Seq[Int], nCopies: Int, series: Seq[SinSeries]) = {
    for {
      timestamp <- timestamps.view
      id <- 1 to nCopies
      SinSeries(name, period, amp) <- series.view
    } yield {
      val x = (timestamp % period).toDouble / period * 2 * math.Pi
      val value = math.sin(x).toFloat * amp
      val tags = Map("period" -> name, "id" -> id.toString, "cluster" -> "test_ds")
      TsPoint(metricName, timestamp, Right(value.toFloat), tags)
    }
  }
}
