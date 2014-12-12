package popeye.inttesting

import java.io.File
import java.util.UUID

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import popeye.Logging
import popeye.pipeline.PipelineCommand
import popeye.pipeline.kafka.KafkaQueueSizeGauge
import popeye.query.QueryCommand
import popeye.test.EmbeddedZookeeper
import popeye.util.{ZkClientConfiguration, KafkaMetaRequests, KafkaOffsetsTracker, ZkConnect}
import scala.concurrent.duration._

import scala.util.Try

class PointToPointBulkLoadTest extends FlatSpec with Matchers with BeforeAndAfter with Logging {

  behavior of "popeye"

  var zookeeper: EmbeddedZookeeper = null
  var kafka: EmbeddedKafka = null
  var hbaseTestingUtility: HBaseTestingUtility = null
  var kafkaZkConnect: ZkConnect = null
  var popeyeZkConnect: ZkConnect = null
  var actorSystem: ActorSystem = null

  before {
    zookeeper = PopeyeIntTestingUtils.createZookeeper(roots = Seq("/kafka", "/popeye"))
    info("zookeeper started")
    kafkaZkConnect = zookeeper.zkConnect.withChroot("/kafka")
    popeyeZkConnect = zookeeper.zkConnect.withChroot("/popeye")
    kafka = EmbeddedKafka.create(
      logsDir = new File("/tmp/kafka-test"),
      zkConnect = kafkaZkConnect,
      port = 9092
    )
    kafka.start()
    kafka.createTopic("popeye-points", partitions = 2)
    hbaseTestingUtility = HBaseTestingUtility.createLocalHTU()
    hbaseTestingUtility.startMiniCluster()
    info("hbase minicluster started")
    val hbaseZk = hbaseTestingUtility.getZkCluster
    val hbaseZkConnect = s"localhost:${hbaseZk.getClientPort}"
    val bulkloadTestTempDir = f"/tmp/bulkload_test/${UUID.randomUUID().toString.replaceAll("-", "")}"
    val outputPath = f"$bulkloadTestTempDir/output"
    val jarPath = f"$bulkloadTestTempDir/jars"
    new File(jarPath).mkdirs()
    val conf = createPopeyeConf(
      kafkaZkConnect = kafkaZkConnect.toZkConnectString,
      hbaseZkConnect = hbaseZkConnect,
      popeyeZkConnect = popeyeZkConnect.toZkConnectString,
      jobOutputPath = outputPath,
      jobJarsPath = jarPath,
      jobRestartPeriod = "10s"
    )
    val metrics = new MetricRegistry()
    actorSystem = ActorSystem("popeye", conf)
    PipelineCommand.run(actorSystem, metrics, conf)
    info("popeye pipeline started")
    QueryCommand.run(actorSystem, metrics, conf)
    info("popeye query started")
  }

  after {
    zookeeper.shutdown()
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
    val currentTime = (System.currentTimeMillis() / 1000).toInt
    val testCase = new PopeyeIntTestCase(currentTime)
    testCase.putPoints("localhost", 4444)
    val brokers = Seq("localhost" -> 9092)
    val kafkaQueueSizeGauge = new KafkaQueueSizeGauge(
      kafkaZkConnect,
      brokers,
      "popeye-opentsdb-reader",
      "popeye-points"
    )
    PopeyeIntTestingUtils.waitWhileKafkaQueueIsNotEmpty(kafkaQueueSizeGauge)
    val kafkaMetaRequests = new KafkaMetaRequests(brokers, "popeye-points-drop")
    val zkClientConf = ZkClientConfiguration(popeyeZkConnect, 5000, 5000)
    val offsetsPath = f"/drop/opentsdb-reader-dropSink/offsets"
    val offsetsTracker = new KafkaOffsetsTracker(kafkaMetaRequests, zkClientConf, offsetsPath)
    def dropQueueIsNonEmpty = {
      val offsetRanges = offsetsTracker.fetchOffsetRanges()
      info(f"drop queue offset ranges: $offsetRanges")
      PopeyeIntTestingUtils.printZkTree(zkClientConf, "/drop")
      offsetRanges.toList
        .forall { case (partitionId, offsetRange) => offsetRange.startOffset < offsetRange.stopOffset}
    }
    while(dropQueueIsNonEmpty) {
      info("waiting for drop queue to be empty")
      Thread.sleep(5000)
    }
    info("waiting for drop queue is empty")
    testCase.runTestQueries("localhost", 8080)
  }


  def createPopeyeConf(kafkaZkConnect: String,
                       hbaseZkConnect: String,
                       popeyeZkConnect: String,
                       jobOutputPath: String,
                       jobJarsPath: String,
                       jobRestartPeriod: String) = {
    val userConfig = ConfigFactory.parseResources("point-to-point-bulk-load-test.conf")
    userConfig
      .withFallback(ConfigFactory.load("reference"))
      .withFallback(ConfigFactory.load())
      .withFallback(ConfigFactory.parseString(
      s"""
          |popeye.kafka.zk.quorum = "$kafkaZkConnect"
          |popeye.hbase.zk.quorum = "$hbaseZkConnect"
          |popeye.popeye.zk.quorum = "$popeyeZkConnect"
          |popeye.job.output.path = "$jobOutputPath"
          |popeye.job.jars.path = "$jobJarsPath"
          |popeye.job.restart.period = "$jobRestartPeriod"
        """.stripMargin))
      .resolve()
  }

}
