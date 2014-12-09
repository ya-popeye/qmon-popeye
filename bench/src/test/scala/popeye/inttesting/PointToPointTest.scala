package popeye.inttesting

import java.io._

import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.zookeeper.CreateMode
import popeye.clients.{SlicerClient, TsPoint, QueryClient}
import popeye.Logging
import popeye.pipeline.kafka.KafkaQueueSizeGauge
import popeye.query.PointsStorage.NameType
import popeye.test.EmbeddedZookeeper
import popeye.util.ZkConnect

import scala.concurrent.duration._
import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import popeye.pipeline.PipelineCommand
import popeye.query.QueryCommand

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class PointToPointTest extends FlatSpec with Matchers with BeforeAndAfter with Logging {
  behavior of "popeye"

  var zookeeper: EmbeddedZookeeper = null
  var kafka: EmbeddedKafka = null
  var hbaseTestingUtility: HBaseTestingUtility = null
  var kafkaZkConnect: String = null
  var actorSystem: ActorSystem = null

  before {
    zookeeper = createZookeeper(roots = Seq("/kafka", "/popeye"))
    info("zookeeper started")
    kafkaZkConnect = zookeeper.connectString + "/kafka"
    val popeyeZkConnect = zookeeper.connectString + "/popeye"
    kafka = EmbeddedKafka.create(
      logsDir = new File("/tmp/kafka-test"),
      zkConnect = ZkConnect.parseString(kafkaZkConnect),
      port = 9092,
      deleteLogsDirContents = true
    )
    kafka.start()
    kafka.createTopic("popeye-points", partitions = 2)
    hbaseTestingUtility = HBaseTestingUtility.createLocalHTU()
    hbaseTestingUtility.startMiniCluster()
    info("hbase minicluster started")
    val hbaseZk = hbaseTestingUtility.getZkCluster
    val hbaseZkConnect = s"localhost:${ hbaseZk.getClientPort }"
    val conf = createPopeyeConf(kafkaZkConnect, hbaseZkConnect, popeyeZkConnect)
    val metrics = new MetricRegistry()
    actorSystem = ActorSystem("popeye", conf)
    PipelineCommand.run(actorSystem, metrics, conf)
    info("popeye pipeline started")
    QueryCommand.run(actorSystem, metrics, conf)
    info("popeye query started")
  }

  after {
    Try(actorSystem.shutdown())
    val hbaseShutdown = Try(hbaseTestingUtility.shutdownMiniCluster())
    info(s"hbase minicluster shutdown: $hbaseShutdown")
    val hbaseDataDeletion = Try {
      hbaseTestingUtility.getDataTestDirOnTestFS()
      hbaseTestingUtility.cleanupTestDir()
    }
    info(s"hbase minicluster data deleted: $hbaseDataDeletion")
    val kafkaShutdown = Try(kafka.shutdown())
    info(s"kafka shutdown: $kafkaShutdown")
    actorSystem.awaitTermination(1 minute)
    info("popeye actor system terminated")
  }

  it should "send points to hbase" in {
    val slicerClient = SlicerClient.createClient("localhost", 4444)
    info("slicer clinet created")
    val currentTime = System.currentTimeMillis() / 1000
    val periods = Seq(1000, 2000, 3000)
    val shifts = Seq(500, 1000, 1500)
    val amps = Seq(10, 20, 30)
    val firstTimestamp = currentTime - 200000
    val timestamps = (firstTimestamp to currentTime by 100).map(_.toInt)
    val points = createSinTsPoints(timestamps, periods, amps, shifts, "popeye")
    slicerClient.putPoints(points)
    info("test points was written to socket output stream")
    slicerClient.commit() should be(true)
    info("test points commited")
    slicerClient.close()
    val kafkaQueueSizeGauge = new KafkaQueueSizeGauge(
      kafkaZkConnect,
      Seq("localhost" -> 9092),
      "popeye-opentsdb-reader",
      "popeye-points"
    )
    val zkClient = zookeeper.newClient
    var kafkaQueueSizeTry: Try[Long] = Failure(new Exception("hasn't run yet"))
    while(kafkaQueueSizeTry != Success(0l)) {
      info(s"waiting for kafka queue to be empty: $kafkaQueueSizeTry")
      if (kafkaQueueSizeTry.isFailure) {
        printZkTree(zkClient, "/kafka")
      }
      Thread.sleep(1000)
      kafkaQueueSizeTry = Try(kafkaQueueSizeGauge.fetchQueueSizes.values.sum)
    }
    info("kafka queue is empty")
    val queryClient = new QueryClient("localhost", 8080)
    val suggestions = queryClient.getSuggestions("s", NameType.MetricType)
    info(s"suggestions response: $suggestions")
    suggestions should equal(Seq("sin"))
    val startTime = firstTimestamp.toInt
    val stopTime = currentTime.toInt + 1
    info(s"startTime: $startTime, stopTime: $stopTime")

    // all timeseries one by one
    for {
      period <- periods
      amp <- amps
      shift <- shifts
    } {
      val tags = Map("period" -> period, "amp" -> amp, "shift" -> shift).mapValues(_.toString) + ("cluster" -> "popeye")
      val query = queryClient.queryJson("sin", "max", tags)
      val results = queryClient.runQuery(startTime, Some(stopTime), Seq(query))
      results.size should equal(1)
      val cond = tagsFilter("amp" -> amp.toString, "period" -> period.toString, "shift" -> shift.toString) _
      val expectedPoints = points
        .filter(point => cond(point.tags))
        .map(point => (point.timestamp, point.value.fold(_.toDouble, _.toDouble)))
      assertEqualTimeseries(results(0).dps, expectedPoints)
    }
    // 'all' aggregation
    {
      val tags = Map("cluster" -> "popeye")
      val query = queryClient.queryJson("sin", "max", tags)
      val results = queryClient.runQuery(startTime, Some(stopTime), Seq(query))
      val expectedPoints = points
        .groupBy(_.timestamp)
        .mapValues(points => points.map(_.value.fold(_.toDouble, _.toDouble)).max)
        .toList
        .sortBy { case (timestamp, _) => timestamp }
      results.size should equal(1)
      assertEqualTimeseries(results(0).dps, expectedPoints)
    }
    // 'groupby' aggregation
    {
      val tags = Map("period" -> "*", "cluster" -> "popeye")
      val query = queryClient.queryJson("sin", "max", tags)
      val results = queryClient.runQuery(startTime, Some(stopTime), Seq(query))
      val expectedPoints = points
        .groupBy(point => point.tags("period"))
        .mapValues {
        points =>
          points.groupBy(_.timestamp)
            .mapValues(points => points.map(_.value.fold(_.toDouble, _.toDouble)).max)
            .toList
            .sortBy { case (timestamp, _) => timestamp }
      }
      for (result <- results) {
        assertEqualTimeseries(result.dps, expectedPoints(result.tags("period")))
      }
    }
  }

  def tagsFilter(filterConds: (String, String)*)(tags: Map[String, String]): Boolean = {
    filterConds.forall {
      case (tagKey, tagValue) => tags(tagKey) == tagValue
    }
  }

  def assertEqualTimeseries(left: Seq[(Int, Double)], right: Seq[(Int, Double)]) = {
    left.size should equal(right.size)
    for (((leftTime, leftValue), (rightTime, rightValue)) <- left zip right) {
      leftTime should equal(rightTime)
      leftValue shouldBe rightValue +- ((math.abs(rightValue) + math.abs(leftValue)) * 0.00001)
    }
  }

  def createSinTsPoints(timestamps: Seq[Int],
                        periods: Seq[Int],
                        amps: Seq[Int],
                        shifts: Seq[Int],
                        cluster: String) = {
    for {
      period <- periods
      amp <- amps
      shift <- shifts
      timestamp <- timestamps
    } yield {
      val x = ((timestamp + shift) % period).toDouble / period * 2 * math.Pi
      val value = math.sin(x).toFloat * amp
      val tags = Map("period" -> period, "amp" -> amp, "shift" -> shift).mapValues(_.toString) + ("cluster" -> "popeye")
      TsPoint("sin", timestamp, Right(value), tags)
    }
  }

  def createPopeyeConf(kafkaZkConnect: String, hbaseZkConnect: String, popeyeZkConnect: String) = {
    val userConfig = ConfigFactory.parseResources("int_test_1.conf")
    userConfig
      .withFallback(ConfigFactory.load("reference"))
      .withFallback(ConfigFactory.load())
      .withFallback(ConfigFactory.parseString(
      s"""
          |popeye.kafka.zk.quorum = "$kafkaZkConnect"
          |popeye.hbase.zk.quorum = "$hbaseZkConnect"
          |popeye.popeye.zk.quorum = "$popeyeZkConnect"
        """.stripMargin))
      .resolve()
  }

  def createZookeeper(roots: Seq[String]): EmbeddedZookeeper = {
    val zookeeper = new EmbeddedZookeeper()
    val zkClient = zookeeper.newClient
    for (root <- roots) {
      zkClient.create(s"$root", "", CreateMode.PERSISTENT)
    }
    zkClient.close()
    zookeeper
  }

  def printZkTree(zkClient: ZkClient, path: String): Unit = {
    info(s"zk dump: $path")
    val children = zkClient.getChildren(path)
    for (child <- children.asScala) {
      printZkTree(zkClient, s"$path/$child")
    }
  }

}
