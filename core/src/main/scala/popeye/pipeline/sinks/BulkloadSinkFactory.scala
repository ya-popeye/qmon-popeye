package popeye.pipeline.sinks

import popeye.pipeline.{PipelineSinkFactory, PointsSink}
import popeye.pipeline.kafka.KafkaSinkFactory
import popeye.util.{OffsetRange, KafkaOffsetsTracker, KafkaMetaRequests, PeriodicExclusiveTask}
import com.typesafe.config.Config
import akka.actor.Scheduler
import scala.concurrent.ExecutionContext
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import scala.concurrent.duration._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import popeye.hadoop.bulkload.BulkloadJobRunner
import popeye.Logging
import scala.collection.JavaConverters._
import java.io.File
import popeye.javaapi.kafka.hadoop.KafkaInput

class BulkloadSinkFactory(kafkaSinkFactory: KafkaSinkFactory,
                          scheduler: Scheduler,
                          execContext: ExecutionContext,
                          storagesConfig: Config) extends PipelineSinkFactory with Logging {
  override def startSink(sinkName: String, config: Config): PointsSink = {
    val jobRunnerConfig = config.getConfig("jobRunner")
    val jobRunnerZkConnect = jobRunnerConfig.getString("zk.quorum")
    val zkSessionTimeout = jobRunnerConfig.getMilliseconds("zk.session.timeout").toInt
    val zkConnectionTimeout = jobRunnerConfig.getMilliseconds("zk.connection.timeout").toInt
    val lockZkClient = new ZkClient(jobRunnerZkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val lockPath = f"/drop/$sinkName/lock"
    val taskPeriod = jobRunnerConfig.getMilliseconds("restart.period").longValue().millis

    val storageConfig = storagesConfig.getConfig(config.getString("storage"))

    val kafkaConfig = config.getConfig("kafka")
    val topic = kafkaConfig.getString("topic")

    val offsetsPath = f"/drop/$sinkName/offsets"
    val hbaseConfiguration = hbaseConf(config.getConfig("hbase"))
    val outputPath = jobRunnerConfig.getString("output.hdfs.path")
    val jarsPath = jobRunnerConfig.getString("jars.hdfs.path")
    val kafkaBrokers = parseBrokerList(kafkaConfig.getString("broker.list"))
    val pointsTableName = storageConfig.getString("table.points")
    val uidTableName = storageConfig.getString("table.uids")

    val hadoopConfigurationPaths = jobRunnerConfig.getStringList("hadoop.conf.paths").asScala
    val hadoopConfiguration = {
      val conf = new Configuration()
      for (path <- hadoopConfigurationPaths) {
        conf.addResource(new File(path).toURI.toURL)
      }
      conf
    }

    PeriodicExclusiveTask.run(lockZkClient, lockPath, scheduler, execContext, taskPeriod) {
      val kafkaMetaRequests = new KafkaMetaRequests(kafkaBrokers, topic)
      val offsetsTracker = new KafkaOffsetsTracker(kafkaMetaRequests, jobRunnerZkConnect, offsetsPath)
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
          hbaseConfiguration,
          pointsTableName,
          uidTableName,
          hadoopConfiguration,
          outputPath,
          jarsPath)
        offsetsTracker.commitOffsets(offsetRanges)
      }
    }
    kafkaSinkFactory.startSink(sinkName, config)
  }

  private def parseBrokerList(str: String) = {
    str.split(",").map {
      brokerStr =>
        val tokens = brokerStr.split(":")
        (tokens(0), tokens(1).toInt)
    }.toSeq
  }

  private def hbaseConf(config: Config) = {
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, config.getString("zk.quorum.hosts"))
    hbaseConfiguration.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, config.getInt("zk.quorum.port"))
    hbaseConfiguration
  }
}
