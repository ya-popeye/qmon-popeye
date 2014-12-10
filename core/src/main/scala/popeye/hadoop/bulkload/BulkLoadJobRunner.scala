package popeye.hadoop.bulkload

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue}
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat2}
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
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import popeye.storage.hbase.TsdbFormatConfig
import popeye.util.KafkaOffsetsTracker.PartitionId
import popeye.util.{ZkConnect, OffsetRange, KafkaOffsetsTracker, KafkaMetaRequests}

class BulkLoadMetrics(prefix: String, metrics: MetricRegistry) {
  val points = metrics.meter(f"$prefix.points")
}

object BulkLoadJobRunner {

  case class HBaseStorageConfig(hBaseZkHostsString: String,
                                hBaseZkPort: Int,
                                pointsTableName: String,
                                uidTableName: String,
                                tsdbFormatConfig: TsdbFormatConfig) {
    def hBaseConfiguration = {
      val conf = HBaseConfiguration.create()
      conf.set(HConstants.ZOOKEEPER_QUORUM, hBaseZkHostsString)
      conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hBaseZkPort)
      conf
    }
  }

  case class ZkClientConfig(zkConnect: ZkConnect, sessionTimeout: Int, connectionTimeout: Int) {
    def zkConnectString = zkConnect.toZkConnectString
  }

  case class JobRunnerConfig(kafkaBrokers: Seq[(String, Int)],
                             topic: String,
                             outputPath: String,
                             jarsPath: String,
                             zkClientConfig: ZkClientConfig,
                             hadoopConfiguration: Configuration) {

  }
}

class BulkLoadJobRunner(name: String,
                        storageConfig: HBaseStorageConfig,
                        runnerConfig: JobRunnerConfig,
                        metrics: BulkLoadMetrics) extends Logging {

  val hadoopConfiguration = runnerConfig.hadoopConfiguration
  val hdfs: FileSystem = FileSystem.get(hadoopConfiguration)
  val outputPath = hdfs.makeQualified(new Path(runnerConfig.outputPath))
  val jarsPath = hdfs.makeQualified(new Path(runnerConfig.jarsPath))
  val offsetsPath = f"/drop/$name/offsets"

  def doBulkload() = {
    val kafkaMetaRequests = new KafkaMetaRequests(runnerConfig.kafkaBrokers, runnerConfig.topic)
    val zkConnectStr = runnerConfig.zkClientConfig.zkConnectString
    val offsetsTracker = new KafkaOffsetsTracker(kafkaMetaRequests, zkConnectStr, offsetsPath)
    val offsetRanges = offsetsTracker.fetchOffsetRanges()
    val kafkaInputs = toKafkaInputs(offsetRanges)

    if (kafkaInputs.nonEmpty) {
      val success = runHadoopJob(kafkaInputs)
      if (success) {
        info("hadoop job finished, starting bulk loading")
        moveHFIlesToHBase()
        info(f"bulkload finished, saving offsets")
        offsetsTracker.commitOffsets(offsetRanges)
        info(f"offsets saved")
      }
    }
  }

  def moveHFIlesToHBase() = {
    // hbase needs rw access
    setPermissionsRecursively(outputPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    val baseConfiguration = storageConfig.hBaseConfiguration
    val hTable = new HTable(baseConfiguration, storageConfig.pointsTableName)
    try {
      new LoadIncrementalHFiles(baseConfiguration).doBulkLoad(outputPath, hTable)
    } finally {
      hTable.close()
    }
  }

  def toKafkaInputs(offsetRanges: Map[PartitionId, OffsetRange]): List[KafkaInput] = {
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

    conf.set(HBASE_CONF_QUORUM, storageConfig.hBaseZkHostsString)
    conf.setInt(HBASE_CONF_QUORUM_PORT, storageConfig.hBaseZkPort)
    conf.set(UNIQUE_ID_TABLE_NAME, storageConfig.uidTableName)
    conf.setInt(UNIQUE_ID_CACHE_SIZE, 100000)
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

    val success = job.waitForCompletion(true)
    if (success) {
      val writtenKeyValues = job.getCounters.findCounter(Counters.MAPPED_KEYVALUES).getValue
      metrics.points.mark(writtenKeyValues)
    } else {
      error(f"hadoop job failed, history url: ${job.getHistoryUrl}")
    }
    success
  }

  def setPermissionsRecursively(path: Path, permission: FsPermission): Unit = {
    hdfs.setPermission(path, permission)
    if (hdfs.isDirectory(path)) {
      for (subPath <- hdfs.listStatus(path).map(_.getPath)) {
        setPermissionsRecursively(subPath, permission)
      }
    }
  }
}
