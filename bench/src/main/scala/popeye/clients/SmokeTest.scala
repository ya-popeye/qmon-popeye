package popeye.clients

import java.io.IOException
import java.util.UUID

import popeye.Logging


object SmokeTest extends Logging {

  def main(args: Array[String]) {
    val currentTime = (System.currentTimeMillis() / 1000).toInt
    val stopTime = currentTime
    val startTime = currentTime - 10000
    val timestamps = startTime to stopTime by 100
    val metric = UUID.randomUUID().toString.replaceAll("-", "")
    info(f"metric: $metric")
    val tags = Map("cluster" -> "popeye_test")
    val points =
      for (timestamp <- timestamps)
      yield {
        TsPoint(metric,
          timestamp,
          Right(math.sin((timestamp % 3000).toDouble / 3000 * 2 * math.Pi).toFloat),
          tags
        )
      }
    val slicerClient = SlicerClient.createClient(args(0), args(1).toInt)
    val queryClient = new QueryClient(args(2), args(3).toInt)
    info(f"sending ${ points.size } points")
    slicerClient.putPoints(points)
    val commitIsOk = slicerClient.commit()
    info(if (commitIsOk) { "commit succeeded" } else { "commit failed" })
    slicerClient.close()

    if (commitIsOk) {
      while(true) {
        try {
          fetchPoints(
            queryClient,
            metric,
            (startTime, stopTime + 1),
            tags
          )
        } catch {
          case e: IOException => info(e)
        }
        Thread.sleep(5000)
      }
    }
  }

  def fetchPoints(client: QueryClient, metric: String, timeRange: (Int, Int), tags: Map[String, String]) = {
    val query = client.queryJson(metric, "avg", tags, None, None)
    val (startTime, stopTime) = timeRange
    val logMessage = client.runQuery(startTime, Some(stopTime), Seq(query))
      .map(result => f"metric: ${ result.metric }, tags: ${ result.tags }, points: ${ result.dps.size }")
      .mkString("\n")
    info("got result:")
    info(logMessage)
  }

}
