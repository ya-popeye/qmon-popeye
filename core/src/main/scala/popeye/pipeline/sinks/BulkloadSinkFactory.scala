package popeye.pipeline.sinks

import popeye.pipeline.{PipelineSinkFactory, PointsSink}
import popeye.pipeline.kafka.KafkaSinkFactory
import popeye.util.PeriodicExclusiveTask
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
    val lockPollInterval = jobRunnerConfig.getMilliseconds("restart.period").longValue().millis

    val storageConfig = storagesConfig.getConfig(config.getString("storage"))

    val kafkaConfig = config.getConfig("kafka")
    val kafkaZkConnect = kafkaConfig.getString("zk.quorum")
    val topic = kafkaConfig.getString("topic")

    val offsetsPath = f"/drop/$sinkName/offsets"
    val hbaseConfiguration = hbaseConf(config.getConfig("hbase"))
    val outputPath = new Path(jobRunnerConfig.getString("outputPath"))
    val brokerList = kafkaConfig.getString("broker.list")
    val tableName = storageConfig.getString("table.points")

    val hadoopConfigurationPaths = jobRunnerConfig.getStringList("hadoop.conf.paths").asScala
    val hadoopConfiguration = {
      val conf = new Configuration()
      for (path <- hadoopConfigurationPaths) {
        conf.addResource(new Path(path))
      }
      conf
    }

    PeriodicExclusiveTask.run(lockZkClient, lockPath, scheduler, execContext, lockPollInterval) {
      BulkloadJobRunner.doBulkload(
        jobRunnerZkConnect,
        offsetsPath,
        kafkaZkConnect,
        brokerList,
        topic,
        hbaseConfiguration,
        tableName,
        hadoopConfiguration,
        outputPath)
    }
    kafkaSinkFactory.startSink(sinkName, config)
  }

  private def hbaseConf(config: Config) = {
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, config.getString("zk.quorum.hosts"))
    hbaseConfiguration.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, config.getInt("zk.quorum.port"))
    hbaseConfiguration
  }
}
