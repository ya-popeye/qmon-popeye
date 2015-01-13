package popeye.pipeline.sinks

import popeye.pipeline.{PipelineSinkFactory, PointsSink}
import popeye.pipeline.kafka.{KafkaSinkStarter, KafkaPointsSinkConfig}
import popeye.storage.hbase.HBaseStorageConfig
import popeye.util._
import com.typesafe.config.Config
import akka.actor.Scheduler
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import popeye.hadoop.bulkload.{BulkLoadMetrics, BulkLoadJobRunner}
import popeye.Logging
import popeye.pipeline.config.KafkaPointsSinkConfigParser
import com.codahale.metrics.MetricRegistry

class BulkloadSinkFactory(sinkFactory: BulkloadSinkStarter,
                          storagesConfig: Config,
                          shardAttributeNames: Set[String]) extends PipelineSinkFactory with Logging {
  override def startSink(sinkName: String, config: Config): PointsSink = {
    val kafkaConfig = KafkaPointsSinkConfigParser.parse(config.getConfig("kafka"))
    val storageName = config.getString("storage")
    val hbaseStorageConfig = {
      val storageConfig = config.getConfig("hbase").withFallback(storagesConfig.getConfig(storageName))
      HBaseStorageConfig.apply(storageConfig, shardAttributeNames, sinkName)
    }
    val hBaseConfig = BulkLoadJobRunner.HBaseStorageConfig(
      hBaseZkConnect = hbaseStorageConfig.zkQuorum,
      pointsTableName = hbaseStorageConfig.pointsTableName,
      uidTableName = hbaseStorageConfig.uidsTableName,
      tsdbFormatConfig = hbaseStorageConfig.tsdbFormatConfig
    )

    val zkClientConfig = {
      val zkConf = config.getConfig("zk")
      ZkClientConfiguration(
        zkConnect = ZkConnect.parseString(zkConf.getString("quorum")),
        sessionTimeout = zkConf.getMilliseconds("session.timeout").toInt,
        connectionTimeout = zkConf.getMilliseconds("connection.timeout").toInt
      )
    }

    val brokerListString = kafkaConfig.producerConfig.brokerList
    val kafkaBrokers = parseBrokerList(brokerListString)
    val jobRunnerConfig = config.getConfig("job")

    val jobConfig = BulkLoadJobRunner.JobRunnerConfig(
      kafkaBrokers = kafkaBrokers,
      topic = kafkaConfig.pointsProducerConfig.topic,
      outputPath = jobRunnerConfig.getString("output.hdfs.path"),
      jarsPath = jobRunnerConfig.getString("jars.hdfs.path"),
      zkClientConfig = zkClientConfig,
      hadoopConfiguration = hbaseStorageConfig.hadoopConfiguration
    )

    val taskPeriod = jobRunnerConfig.getMilliseconds("restart.period").longValue().millis
    sinkFactory.startSink(sinkName, BulkloadSinkConfig(kafkaConfig, hBaseConfig, jobConfig, taskPeriod))
  }

  private def parseBrokerList(str: String): Seq[(String, Int)] = {
    str.split(",").map {
      brokerStr =>
        val tokens = brokerStr.split(":")
        (tokens(0), tokens(1).toInt)
    }.toSeq
  }

}

case class BulkloadSinkConfig(kafkaSinkConfig: KafkaPointsSinkConfig,
                              hBaseConfig: BulkLoadJobRunner.HBaseStorageConfig,
                              jobConfig: BulkLoadJobRunner.JobRunnerConfig,
                              taskPeriod: FiniteDuration)

class BulkloadSinkStarter(kafkaSinkFactory: KafkaSinkStarter,
                          scheduler: Scheduler,
                          execContext: ExecutionContext,
                          metrics: MetricRegistry) extends Logging {
  def startSink(name: String, config: BulkloadSinkConfig) = {

    val BulkloadSinkConfig(kafkaConfig, hBaseConfig, jobConfig, taskPeriod) = config

    val zkClientConfig = jobConfig.zkClientConfig

    val lockPath = f"/drop/$name/lock"
    val bulkLoadMetrics = new BulkLoadMetrics("bulkload", metrics)
    val bulkLoadJobRunner = new BulkLoadJobRunner(name, hBaseConfig, jobConfig, bulkLoadMetrics)

    PeriodicExclusiveTask.run(zkClientConfig, lockPath, scheduler, execContext, taskPeriod) {
      bulkLoadJobRunner.doBulkload()
    }

    kafkaSinkFactory.startSink(name, kafkaConfig)
  }
}
