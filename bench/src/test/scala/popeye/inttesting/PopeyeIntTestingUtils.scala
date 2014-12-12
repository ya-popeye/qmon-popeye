package popeye.inttesting

import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.CreateMode
import popeye.Logging
import popeye.pipeline.kafka.KafkaQueueSizeGauge
import popeye.test.EmbeddedZookeeper
import popeye.util.ZkClientConfiguration
import scala.collection.JavaConverters._

import scala.util.{Failure, Success, Try}

object PopeyeIntTestingUtils extends Logging {
  def waitWhileKafkaQueueIsNotEmpty(kafkaQueueSizeGauge: KafkaQueueSizeGauge) = {
    var kafkaQueueSizeTry: Try[Long] = Failure(new Exception("hasn't run yet"))
    while(kafkaQueueSizeTry != Success(0l)) {
      info(s"waiting for kafka queue to be empty: $kafkaQueueSizeTry")
      Thread.sleep(1000)
      kafkaQueueSizeTry = Try(kafkaQueueSizeGauge.fetchQueueSizes.values.sum)
    }
    info("kafka queue is empty")
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

  def printZkTree(zkClientConf: ZkClientConfiguration, path: String): Unit = {
    def printZkTreeInner(zkClient: ZkClient, path: String): Unit = {
      info(s"zk dump: $path")
      val children = zkClient.getChildren(path)
      for (child <- children.asScala) {
        printZkTreeInner(zkClient, s"$path/$child")
      }
    }
    val zkClient = zkClientConf.createClient
    try {
      printZkTreeInner(zkClient, path)
    } finally {
      zkClient.close()
    }
  }
}
