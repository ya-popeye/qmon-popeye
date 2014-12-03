package popeye.query

import java.io.File
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.{TimeUnit, Executors}

import akka.dispatch.ExecutionContexts
import com.codahale.metrics.{CsvReporter, ConsoleReporter, MetricRegistry}
import org.apache.hadoop.hbase.DaemonThreadFactory
import popeye.{Instrumented, Logging}
import popeye.clients.{TsPoint, QueryClient, SlicerClient}
import popeye.pipeline.MetricGenerator

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.Random

object QueryBench extends Logging with Instrumented {

  def main(args: Array[String]) {
    val consoleReporter = ConsoleReporter
      .forRegistry(metricRegistry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build()
    consoleReporter.start(1, TimeUnit.SECONDS)

    val metricsDir = new File("query_bench_metrics")
    metricsDir.mkdirs()

    val csvReporter = CsvReporter
      .forRegistry(metricRegistry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(metricsDir)

    csvReporter.start(5, TimeUnit.SECONDS)

    args(0) match {
      case "put" => sendPointsToSlicer(args.tail)
      case "read" => benchQuery(args.tail)
    }
  }

  val metricRegistry = new MetricRegistry

  val sentPoints = metrics.meter("send.points")

  val sendTime = metrics.timer("send.time")

  val querySingleTsTime = metrics.timer("query.single.ts.time")
  val querySingleTagValueTime = metrics.timer("query.single.host.time")
  val queryAllTagValuesTime = metrics.timer("query.all.tags.time")

  val metricNames = MetricGenerator.metrics

  val hostTagValues = (0 until 100).map(i => f"w$i.qmon.yandex.net")
  val threadTagValues = (0 until 50).map(i => f"thread-$i")
  val pointsPerSeries = 1000
  val timeStep = 300

  val clusterTagValue = "test"

  val allTagValues = Seq("host" -> hostTagValues, "thread" -> threadTagValues, "cluster" -> Seq(clusterTagValue))

  val timeseriesIds: IndexedSeq[(String, Map[String, String])] = {
    for {
      metric <- metricNames
      tags <- MetricGenerator.generateTags(allTagValues).map(_.toMap)
    } yield {
      (metric, tags)
    }
  }

  def sendPointsToSlicer(args: Array[String]) = {
    val Array(slicerHost, slicerPort) = args(0).split(":")
    val currentTime = args(1).toInt
    val metricNamePrefix = args(2)
    val chunks = timeseriesIds.grouped(10000)
    implicit val executionContext = ExecutionContexts.fromExecutor(
      Executors.newFixedThreadPool(30, new DaemonThreadFactory("SlicerClient"))
    )
    val startTime = currentTime - pointsPerSeries * timeStep
    val timestamps = (0 until pointsPerSeries).map(i => startTime + i * timeStep)
    val futures = chunks.map {
      chunk =>
        val prefixedChunk = chunk.map {
          case (metricName, tags) => (metricNamePrefix + metricName, tags)
        }
        Future {
          putPoints(slicerHost, slicerPort.toInt, prefixedChunk, timestamps)
        }
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  def putPoints(slicerHost: String,
                slicerPort: Int,
                timeseriesIds: Seq[(String, Map[String, String])],
                timestamps: Seq[Int]) = {
    val slicerClient = SlicerClient.createClient(slicerHost, slicerPort.toInt)
    val random = new Random()
    for ((metric, tags) <- timeseriesIds) {
      val points = timestamps.map {
        time => TsPoint(metric, time, Right(random.nextFloat()), tags)
      }
      sendTime.time {
        slicerClient.putPoints(points)
        assert(slicerClient.commit(), "commit failed")
      }
      sentPoints.mark(points.size)
    }
  }

  case class QueryBenchConfig(host: String,
                              port: Int,
                              currentTime: Int,
                              metricNamePrefix: String,
                              clientsCount: Int,
                              requestType: String)

  val queryBenchConfigParser = new scopt.OptionParser[QueryBenchConfig]("popeye-run-class.sh popeye.query.QueryBench") {
    head("popeye query load test tool", "0.1")
    opt[String]("address") valueName "<query address>" action {
      (param, config) =>
        val List(host, port) = param.split(":").toList
        config.copy(host = host, port = port.toInt)
    }
    opt[Int]("time") valueName "<current time>" action {
      (param, config) => config.copy(currentTime = param)
    }
    opt[String]("metric_prefix") valueName "<metric prefix>" action {
      (param, config) => config.copy(metricNamePrefix = param)
    }
    opt[Int]("clients") valueName "<client count>" action {
      (param, config) => config.copy(clientsCount = param)
    }
    opt[String]("request_type") valueName "<request type>" action {
      (param, config) => config.copy(requestType = param)
    }
  }

  def benchQuery(args: Array[String]) = {
    val default = QueryBenchConfig(
      host = "localhost",
      port = 8080,
      currentTime = (System.currentTimeMillis() / 1000).toInt,
      metricNamePrefix = "",
      clientsCount = 10,
      requestType = "single"
    )
    val config = queryBenchConfigParser.parse(args, default).getOrElse {
      System.exit(1)
      ???
    }
    val exct = ExecutionContexts.fromExecutor(
      Executors.newFixedThreadPool(config.clientsCount, new DaemonThreadFactory("QueryClient"))
    )
    val startTime = config.currentTime - 3600 * 24
    val timeInterval = (startTime, config.currentTime + 1)
    val benchStrategy: () => Future[Unit] = config.requestType match {
      case "single" =>
        () => randomSingleTimeseriesRequests(config.host, config.port, config.metricNamePrefix, timeInterval)(exct)
      case "host" =>
        () => randomByTagAggregationRequests(
          config.host,
          config.port,
          config.metricNamePrefix,
          timeInterval,
          "host",
          hostTagValues)(exct)
      case "thread" =>
        () => randomByTagAggregationRequests(
          config.host,
          config.port,
          config.metricNamePrefix,
          timeInterval,
          "thread",
          threadTagValues)(exct)
      case "all" =>
        () => randomMetricRequests(
          config.host,
          config.port,
          config.metricNamePrefix,
          timeInterval)(exct)
    }
    val futures = List.fill(config.clientsCount) {
      benchStrategy()
    }
    implicit val ectx = ExecutionContext.global
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  def randomSingleTimeseriesRequests(queryHost: String,
                                     queryPort: Int,
                                     metricsNamePrefix: String,
                                     timeInterval: (Int, Int))
                                    (implicit exct: ExecutionContext): Future[Unit] = Future {
    val queryClient = new QueryClient(queryHost, queryPort)
    val random = new Random
    while(true) {
      val (metricName, tags) = timeseriesIds(random.nextInt(timeseriesIds.size))
      val query = queryClient.queryJson(
        metricName = metricsNamePrefix + metricName,
        aggregator = "avg",
        tags = tags
      )
      val (startTime, stopTime) = timeInterval
      val result = querySingleTsTime.time {
        queryClient.runQuery(startTime, Some(stopTime), Seq(query))
      }
      val resultSize = result.headOption.map(_.dps.size)
      info(f"single timeseries ${ (metricName, tags) } response size: $resultSize ")
    }
  }

  def randomMetricRequests(queryHost: String,
                           queryPort: Int,
                           metricsNamePrefix: String,
                           timeInterval: (Int, Int))
                          (implicit exct: ExecutionContext): Future[Unit] = Future {
    val queryClient = new QueryClient(queryHost, queryPort)
    val random = new Random
    while(true) {
      val metricName = metricNames(random.nextInt(metricNames.size))
      val query = queryClient.queryJson(
        metricName = metricsNamePrefix + metricName,
        aggregator = "avg",
        tags = Map("cluster" -> clusterTagValue)
      )
      val (startTime, stopTime) = timeInterval
      val result = querySingleTsTime.time {
        queryClient.runQuery(startTime, Some(stopTime), Seq(query))
      }
      val resultSize = result.headOption.map(_.dps.size)
      info(f"metric ${ metricName } response size: $resultSize ")
    }
  }

  def randomByTagAggregationRequests(queryHost: String,
                                     queryPort: Int,
                                     metricsNamePrefix: String,
                                     timeInterval: (Int, Int),
                                     tagKey: String,
                                     tagValues: IndexedSeq[String])
                                    (implicit exct: ExecutionContext): Future[Unit] = Future {
    val queryClient = new QueryClient(queryHost, queryPort)
    val random = new Random
    while(true) {
      val metricName = metricNames(random.nextInt(metricNames.size))
      val randomTagValue = tagValues(random.nextInt(tagValues.size))
      val tags = Map(tagKey -> randomTagValue, "cluster" -> clusterTagValue)
      val query = queryClient.queryJson(
        metricName = metricsNamePrefix + metricName,
        aggregator = "avg",
        tags = tags
      )
      val (startTime, stopTime) = timeInterval
      val result = querySingleTagValueTime.time {
        queryClient.runQuery(startTime, Some(stopTime), Seq(query))
      }
      val resultSize = result.headOption.map(_.dps.size)
      info(f"aggregation by host ${ (metricName, tags) } response size: $resultSize ")
    }
  }


}
