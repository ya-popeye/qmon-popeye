package popeye.pipeline.sinks

import popeye.pipeline.{PipelineSinkFactory, PointsSink}
import popeye.pipeline.kafka.{KafkaSinkStarter, KafkaPointsSinkConfig}
import popeye.util.{OffsetRange, KafkaOffsetsTracker, KafkaMetaRequests, PeriodicExclusiveTask}
import com.typesafe.config.Config
import akka.actor.Scheduler
import scala.concurrent.ExecutionContext
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import scala.concurrent.duration._
import org.apache.hadoop.conf.Configuration
import popeye.hadoop.bulkload.BulkloadJobRunner
import popeye.Logging
import scala.collection.JavaConverters._
import java.io.File
import popeye.javaapi.kafka.hadoop.KafkaInput
import popeye.pipeline.config.KafkaPointsSinkConfigParser

class BulkloadSinkFactory(sinkFactory: BulkloadSinkStarter,
                          storagesConfig: Config) extends PipelineSinkFactory with Logging {
  override def startSink(sinkName: String, config: Config): PointsSink = {
    val kafkaConfig = KafkaPointsSinkConfigParser.parse(config.getConfig("kafka"))

    val storageConfig = storagesConfig.getConfig(config.getString("storage"))
    val hBaseConfig = BulkloadJobRunner.HBaseStorageConfig(
      hBaseZkHostsString = config.getString("hbase.zk.quorum.hosts"),
      hBaseZkPort = config.getInt("hbase.zk.quorum.port"),
      pointsTableName = storageConfig.getString("table.points"),
      uidTableName = storageConfig.getString("table.uids")
    )

    val jobRunnerConfig = config.getConfig("jobRunner")

    val jobConfig = BulkloadSinkConfig.JobRunnerConfig(
      taskPeriod = jobRunnerConfig.getMilliseconds("restart.period").longValue().millis,
      outputPath = jobRunnerConfig.getString("output.hdfs.path"),
      jarsPath = jobRunnerConfig.getString("jars.hdfs.path"),
      zkConnect = jobRunnerConfig.getString("zk.quorum"),
      zkSessionTimeout = jobRunnerConfig.getMilliseconds("zk.session.timeout").toInt,
      zkConnectionTimeout = jobRunnerConfig.getMilliseconds("zk.connection.timeout").toInt,
      hadoopConfigurationPaths = jobRunnerConfig.getStringList("hadoop.conf.paths").asScala
    )

    sinkFactory.startSink(sinkName, BulkloadSinkConfig(kafkaConfig, hBaseConfig, jobConfig))
  }

}

object BulkloadSinkConfig {

  case class JobRunnerConfig(taskPeriod: FiniteDuration,
                             outputPath: String,
                             jarsPath: String,
                             zkConnect: String,
                             zkSessionTimeout: Int,
                             zkConnectionTimeout: Int,
                             hadoopConfigurationPaths: Seq[String]) {

    def hadoopConfiguration = {
      val conf = new Configuration()
      for (path <- hadoopConfigurationPaths) {
        conf.addResource(new File(path).toURI.toURL)
      }
      conf
    }
  }

}

case class BulkloadSinkConfig(kafkaSinkConfig: KafkaPointsSinkConfig,
                              hBaseConfig: BulkloadJobRunner.HBaseStorageConfig,
                              jobConfig: BulkloadSinkConfig.JobRunnerConfig)

class BulkloadSinkStarter(kafkaSinkFactory: KafkaSinkStarter,
                          scheduler: Scheduler,
                          execContext: ExecutionContext) {
  def startSink(name: String, config: BulkloadSinkConfig) = {

    val BulkloadSinkConfig(kafkaConfig, hBaseConfig, jobConfig) = config

    val lockZkClient = new ZkClient(
      jobConfig.zkConnect,
      jobConfig.zkSessionTimeout,
      jobConfig.zkConnectionTimeout,
      ZKStringSerializer)

    val lockPath = f"/drop/$name/lock"
    val offsetsPath = f"/drop/$name/offsets"

    PeriodicExclusiveTask.run(lockZkClient, lockPath, scheduler, execContext, jobConfig.taskPeriod) {
      val brokerListString = kafkaConfig.producerConfig.brokerList
      val topic = kafkaConfig.pointsProducerConfig.topic
      val kafkaBrokers = parseBrokerList(brokerListString)
      val kafkaMetaRequests = new KafkaMetaRequests(kafkaBrokers, topic)
      val offsetsTracker = new KafkaOffsetsTracker(kafkaMetaRequests, jobConfig.zkConnect, offsetsPath)
      val offsetRanges = offsetsTracker.fetchOffsetRanges()

      val kafkaInputs = offsetRanges.toList.map {
        case (partitionId, OffsetRange(startOffset, stopOffset)) =>
          KafkaInput(
            topic,
            partitionId,
            startOffset,
            stopOffset
          )
      }.filterNot(_.isEmpty)

      if (kafkaInputs.nonEmpty) {
        BulkloadJobRunner.doBulkload(
          kafkaInputs,
          kafkaBrokers,
          hBaseConfig,
          jobConfig.hadoopConfiguration,
          jobConfig.outputPath,
          jobConfig.jarsPath)
        offsetsTracker.commitOffsets(offsetRanges)
      }
    }
    kafkaSinkFactory.startSink(name, kafkaConfig)
  }

  private def parseBrokerList(str: String) = {
    str.split(",").map {
      brokerStr =>
        val tokens = brokerStr.split(":")
        (tokens(0), tokens(1).toInt)
    }.toSeq
  }

}
