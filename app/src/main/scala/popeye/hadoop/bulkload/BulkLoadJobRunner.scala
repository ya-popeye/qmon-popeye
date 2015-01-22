package popeye.hadoop.bulkload

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{TableName, KeyValue}
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import popeye.javaapi.kafka.hadoop.KafkaInput
import org.apache.hadoop.mapred.JobConf
import popeye.hadoop.bulkload.BulkLoadConstants._
import popeye.Logging
import popeye.hadoop.bulkload.BulkLoadJobRunner.{JobRunnerConfig, HBaseStorageConfig}
import com.codahale.metrics.MetricRegistry
import popeye.storage.hbase.TsdbFormatConfig
import popeye.util._
import popeye.util.hbase.HBaseConfigured

class BulkLoadMetrics(prefix: String, metrics: MetricRegistry) {
  val points = metrics.meter(f"$prefix.points")
  val runningJobs = metrics.counter(f"$prefix.jobs")
}

object BulkLoadJobRunner {

  val jobName = "popeye_bulkload"

  case class HBaseStorageConfig(hBaseZkConnect: ZkConnect,
                                pointsTableName: TableName,
                                uidTableName: String,
                                tsdbFormatConfig: TsdbFormatConfig) {
    def hBaseConfiguration = {
      new HBaseConfigured(ConfigFactory.empty(), hBaseZkConnect).hbaseConfiguration
    }
  }


  case class JobRunnerConfig(kafkaBrokers: Seq[(String, Int)],
                             topic: String,
                             outputPath: String,
                             jarsPath: String,
                             zkClientConfig: ZkClientConfiguration,
                             hadoopConfiguration: Configuration)

}

class BulkLoadJobRunner(name: String,
                        storageConfig: HBaseStorageConfig,
                        runnerConfig: JobRunnerConfig,
                        metrics: BulkLoadMetrics) extends Logging {

  def doBulkload() = {
    val hadoopConfiguration = runnerConfig.hadoopConfiguration
    val hdfs: FileSystem = FileSystem.newInstance(hadoopConfiguration)
    try {
      val context = new BulkLoadContext(hdfs, name, storageConfig, runnerConfig, metrics)
      context.doBulkload()
    } finally {
      hdfs.close()
    }
  }
}

class BulkLoadContext(hdfs: FileSystem,
                      name: String,
                      storageConfig: HBaseStorageConfig,
                      runnerConfig: JobRunnerConfig,
                      metrics: BulkLoadMetrics) extends Logging {

  val hadoopConfiguration = runnerConfig.hadoopConfiguration
  val outputPath = hdfs.makeQualified(new Path(runnerConfig.outputPath))
  val jarsPath = hdfs.makeQualified(new Path(runnerConfig.jarsPath))
  val offsetsPath = f"/drop/$name/offsets"

  def doBulkload() = {
    info("initiating bulk loading")
    val kafkaMetaRequests = new KafkaMetaRequests(runnerConfig.kafkaBrokers, runnerConfig.topic)
    val offsetsTracker = new KafkaOffsetsTracker(kafkaMetaRequests, runnerConfig.zkClientConfig, offsetsPath)
    val offsetRanges = offsetsTracker.fetchOffsetRanges()
    info(f"kafka offset fetched: $offsetRanges")
    val kafkaInputs = toKafkaInputs(offsetRanges)
    info(f"input data: $kafkaInputs")
    if (kafkaInputs.nonEmpty) {
      val success = runHadoopJob(kafkaInputs)
      if (success) {
        info("hadoop job finished, starting bulk loading")
        BulkloadUtils.moveHFilesToHBase(
          hdfs,
          outputPath,
          storageConfig.hBaseConfiguration,
          storageConfig.pointsTableName
        )
        info(f"bulkload finished, saving offsets")
        offsetsTracker.commitOffsets(offsetRanges)
        info(f"offsets saved")
      }
    }
  }

  def toKafkaInputs(offsetRanges: Map[Int, OffsetRange]): List[KafkaInput] = {
    val kafkaInputs = offsetRanges.toList.map {
      case (partitionId, OffsetRange(startOffset, stopOffset)) =>
        KafkaInput(
          runnerConfig.topic,
          partitionId,
          startOffset,
          stopOffset
        )
    }.filterNot(_.isEmpty)
    kafkaInputs
  }

  private def runHadoopJob(kafkaInputs: Seq[KafkaInput]) = {
    info(f"inputs: $kafkaInputs, hbase config:$storageConfig," +
      f" outPath:$outputPath, brokers:${ runnerConfig.kafkaBrokers }")
    hdfs.delete(outputPath, true)
    val conf: JobConf = new JobConf(hadoopConfiguration)
    conf.set(KAFKA_INPUTS, KafkaInput.renderInputsString(kafkaInputs))
    val brokersListString = runnerConfig.kafkaBrokers.map { case (host, port) => f"$host:$port" }.mkString(",")
    conf.set(KAFKA_BROKERS, brokersListString)
    conf.setInt(KAFKA_CONSUMER_TIMEOUT, 5000)
    conf.setInt(KAFKA_CONSUMER_BUFFER_SIZE, 100000)
    conf.setInt(KAFKA_CONSUMER_FETCH_SIZE, 2000000)
    conf.set(KAFKA_CLIENT_ID, "drop")

    conf.set(HBASE_ZK_CONNECT, storageConfig.hBaseZkConnect.toZkConnectString)
    conf.set(UNIQUE_ID_TABLE_NAME, storageConfig.uidTableName)
    conf.setInt(UNIQUE_ID_CACHE_SIZE, 100000)
    conf.setInt(MAX_DELAYED_POINTS, 100000)
    val tsdbFormatConfigString = {
      val config = TsdbFormatConfig.renderConfig(storageConfig.tsdbFormatConfig)
      config.root().render()
    }
    conf.set(TSDB_FORMAT_CONFIG, tsdbFormatConfigString)

    val hTable = new HTable(storageConfig.hBaseConfiguration, storageConfig.pointsTableName)
    val job: Job = Job.getInstance(conf)

    val jars = FileSystem.get(hadoopConfiguration).listStatus(jarsPath)

    for (jar <- jars) {
      val path = jar.getPath
      job.addFileToClassPath(path)
    }

    job.setInputFormatClass(classOf[PopeyePointsKafkaTopicInputFormat])
    job.setMapperClass(classOf[PointsToKeyValueMapper])
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, hTable)
    // HFileOutputFormat2.configureIncrementalLoad abuses tmpjars
    job.getConfiguration.unset("tmpjars")
    FileOutputFormat.setOutputPath(job, outputPath)
    job.setJobName(BulkLoadJobRunner.jobName)
    val success = try {
      info("submitting job")
      metrics.runningJobs.inc()
      job.submit()
      val trackingUrl = getTrackingUrlWithReties(job, retries = 3)
      info(f"job submitted, tracking url: '$trackingUrl'")
      job.waitForCompletion(true)
    } finally {
      metrics.runningJobs.dec()
    }
    if (success) {
      val writtenKeyValues = job.getCounters.findCounter(Counters.MAPPED_KEYVALUES).getValue
      metrics.points.mark(writtenKeyValues)
    } else {
      error(f"hadoop job failed, history url: ${job.getHistoryUrl}")
    }
    success
  }

  def getTrackingUrlWithReties(job: Job, retries: Int): String = {
    val noUrl = "N/A"
    for (_ <- 0 to retries) {
      val trackingUrl = job.getStatus.getTrackingUrl
      info(f"trying to get tracking url, got: $trackingUrl")
      if (trackingUrl != noUrl) {
        return trackingUrl
      }
      Thread.sleep(500)
      info("retrying to get tracking url")
    }
    noUrl
  }
}
