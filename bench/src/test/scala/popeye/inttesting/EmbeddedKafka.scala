package popeye.inttesting

import java.io.{IOException, File}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServerStartable}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import popeye.Logging
import popeye.util.ZkConnect

class EmbeddedKafka(kafka: KafkaServerStartable) extends Logging {
  def start() = {
    info("starting kafka")
    kafka.startup()
    info("kafka was started")
  }

  def shutdown() = kafka.shutdown()

  def createTopic(topic: String, partitions: Int) = {
    info(f"creating kafka topic $topic")
    val zkClient = new ZkClient(kafka.serverConfig.zkConnect, 5000, 5000, ZKStringSerializer)
    AdminUtils.createTopic(
      zkClient = zkClient,
      topic = topic,
      partitions = partitions,
      replicationFactor = 1
    )
    info("kafka topic created")
  }
}

object EmbeddedKafka {
  def create(logsDir: File,
             zkConnect: ZkConnect,
             port: Int,
             deleteLogsDirContents: Boolean): EmbeddedKafka = {
    if (deleteLogsDirContents && logsDir.exists()) {
      rmDir(logsDir)
    }
    val kafkaProperties = new Properties()
    kafkaProperties.put("log.dir", logsDir.getCanonicalPath)
    kafkaProperties.put("zookeeper.connect", zkConnect.toZkConnectString)
    kafkaProperties.put("broker.id", "1")
    kafkaProperties.put("port", port.toString)
    val kafkaConfig = new KafkaConfig(kafkaProperties)
    val kafka = new KafkaServerStartable(kafkaConfig)
    new EmbeddedKafka(kafka)
  }

  def rmDir(path: File) = {
    Files.walkFileTree(Paths.get(path.toURI), new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
  }
}
