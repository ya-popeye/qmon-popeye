package popeye.inttesting

import java.io._

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.zookeeper.CreateMode
import popeye.Logging
import popeye.pipeline.kafka.KafkaQueueSizeGauge
import popeye.test.EmbeddedZookeeper
import popeye.util.ZkConnect

import scala.concurrent.duration._
import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import popeye.pipeline.PipelineCommand
import popeye.query.QueryCommand

import scala.util.Try

class PointToPointTest extends FlatSpec with Matchers with BeforeAndAfter with Logging {
  behavior of "popeye"

  var zookeeper: EmbeddedZookeeper = null
  var kafka: EmbeddedKafka = null
  var hbaseTestingUtility: HBaseTestingUtility = null
  var kafkaZkConnect: ZkConnect = null
  var actorSystem: ActorSystem = null

  before {
    zookeeper = PopeyeIntTestingUtils.createZookeeper(roots = Seq("/kafka", "/popeye"))
    info("zookeeper started")
    kafkaZkConnect = zookeeper.zkConnect.withChroot("/kafka")
    val popeyeZkConnect = zookeeper.connectString + "/popeye"
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
    val hbaseZkConnect = s"localhost:${ hbaseZk.getClientPort }"
    val conf = createPopeyeConf(kafkaZkConnect.toZkConnectString, hbaseZkConnect, popeyeZkConnect)
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
    val kafkaQueueSizeGauge = new KafkaQueueSizeGauge(
      kafkaZkConnect,
      Seq("localhost" -> 9092),
      "popeye-opentsdb-reader",
      "popeye-points"
    )
    PopeyeIntTestingUtils.waitWhileKafkaQueueIsNotEmpty(kafkaQueueSizeGauge)
    testCase.runTestQueries("localhost", 8080)
  }

  def createPopeyeConf(kafkaZkConnect: String, hbaseZkConnect: String, popeyeZkConnect: String) = {
    val userConfig = ConfigFactory.parseResources("point-to-point-test.conf")
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
}
