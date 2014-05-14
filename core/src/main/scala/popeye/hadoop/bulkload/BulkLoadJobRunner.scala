package popeye.hadoop.bulkload

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue}
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat2}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.Configuration
import popeye.javaapi.kafka.hadoop.KafkaInput
import org.apache.hadoop.mapred.JobConf
import popeye.hadoop.bulkload.BulkLoadConstants._
import popeye.Logging
import popeye.hadoop.bulkload.BulkLoadJobRunner.HBaseStorageConfig
import com.codahale.metrics.MetricRegistry

class BulkLoadMetrics(prefix: String, metrics: MetricRegistry) {
  val points = metrics.meter(f"$prefix.points")
}

object BulkLoadJobRunner {

  case class HBaseStorageConfig(hBaseZkHostsString: String,
                                hBaseZkPort: Int,
                                pointsTableName: String,
                                uidTableName: String) {
    def hBaseConfiguration = {
      val conf = HBaseConfiguration.create()
      conf.set(HConstants.ZOOKEEPER_QUORUM, hBaseZkHostsString)
      conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hBaseZkPort)
      conf
    }
  }

}

class BulkLoadJobRunner(kafkaBrokers: Seq[(String, Int)],
                        storageConfig: HBaseStorageConfig,
                        hadoopConfiguration: Configuration,
                        outputHdfsPath: String,
                        jarsHdfsPath: String,
                        metrics: BulkLoadMetrics) extends Logging {

  val hdfs: FileSystem = FileSystem.get(hadoopConfiguration)
  val outputPath = hdfs.makeQualified(new Path(outputHdfsPath))
  val jarsPath = hdfs.makeQualified(new Path(jarsHdfsPath))

  def doBulkload(kafkaInputs: Seq[KafkaInput]) {

    val success = runHadoopJob(kafkaInputs)
    if (success) {
      info("hadoop job finished, bulkload starting")
      val baseConfiguration = storageConfig.hBaseConfiguration
      val hTable = new HTable(baseConfiguration, storageConfig.pointsTableName)
      try {
        new LoadIncrementalHFiles(baseConfiguration).doBulkLoad(outputPath, hTable)
      } finally {
        hTable.close()
      }
    }
  }

  private def runHadoopJob(kafkaInputs: Seq[KafkaInput]) = {
    info(f"inputs: $kafkaInputs, hbase config:$storageConfig, outPath:$outputPath, brokers:$kafkaBrokers")
    hdfs.delete(outputPath, true)
    val conf: JobConf = new JobConf(hadoopConfiguration)
    conf.set(KAFKA_INPUTS, KafkaInput.renderInputsString(kafkaInputs))
    val brokersListString = kafkaBrokers.map {case (host, port) => f"$host:$port"}.mkString(",")
    conf.set(KAFKA_BROKERS, brokersListString)
    conf.setInt(KAFKA_CONSUMER_TIMEOUT, 5000)
    conf.setInt(KAFKA_CONSUMER_BUFFER_SIZE, 100000)
    conf.setInt(KAFKA_CONSUMER_FETCH_SIZE, 2000000)
    conf.set(KAFKA_CLIENT_ID, "drop")

    conf.set(HBASE_CONF_QUORUM, storageConfig.hBaseZkHostsString)
    conf.setInt(HBASE_CONF_QUORUM_PORT, storageConfig.hBaseZkPort)
    conf.set(UNIQUE_ID_TABLE_NAME, storageConfig.uidTableName)
    conf.setInt(UNIQUE_ID_CACHE_SIZE, 100000)

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
      warn(f"hadoop job failed, history url: ${job.getHistoryUrl}")
    }
    success
  }

}
