package popeye.hadoop.bulkload

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HConstants, KeyValue}
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
import popeye.util.{OffsetRange, KafkaMetaRequests, KafkaOffsetsTracker}

object BulkloadJobRunner extends Logging {

  def doBulkload(kafkaInputs: Seq[KafkaInput],
                 kafkaBrokers: Seq[(String, Int)],
                 hbaseConfiguration: Configuration,
                 pointsTableName: String,
                 uIdsTableName: String,
                 hadoopConfiguration: Configuration,
                 outputHdfsPath: String,
                 jarsHdfsPath: String) {
    val hdfs: FileSystem = FileSystem.get(hadoopConfiguration)
    val outputPath = hdfs.makeQualified(new Path(outputHdfsPath))
    val jarsPath = hdfs.makeQualified(new Path(jarsHdfsPath))

    runHadoopJob(
      hadoopConfiguration,
      hbaseConfiguration,
      kafkaBrokers,
      pointsTableName,
      uIdsTableName,
      kafkaInputs,
      outputPath,
      jarsPath
    )
    info("hadoop job finished, bulkload starting")
    val hTable = new HTable(hbaseConfiguration, pointsTableName)
    try {
      new LoadIncrementalHFiles(hbaseConfiguration).doBulkLoad(outputPath, hTable)
    } finally {
      hTable.close()
    }
    info(f"bulkload finished, saving offsets")
    info(f"offsets saved")
  }

  private def runHadoopJob(hadoopConfiguration: Configuration,
                           hbaseConfiguration: Configuration,
                           kafkaBrokers: Seq[(String, Int)],
                           pointsTableName: String,
                           uidsTableName: String,
                           kafkaInputs: Seq[KafkaInput],
                           outputPath: Path,
                           jarsPath: Path) = {
    info(f"inputs: $kafkaInputs, points table :$pointsTableName, uid table: $uidsTableName, outPath:$outputPath, brokers:$kafkaBrokers")
    FileSystem.get(hadoopConfiguration).delete(outputPath, true)
    val conf: JobConf = new JobConf(hadoopConfiguration)
    conf.set(KAFKA_INPUTS, KafkaInput.renderInputsString(kafkaInputs))
    val brokersListString = kafkaBrokers.map {case (host, port) => f"$host:$port"}.mkString(",")
    conf.set(KAFKA_BROKERS, brokersListString)
    conf.setInt(KAFKA_CONSUMER_TIMEOUT, 5000)
    conf.setInt(KAFKA_CONSUMER_BUFFER_SIZE, 100000)
    conf.setInt(KAFKA_CONSUMER_FETCH_SIZE, 2000000)
    conf.set(KAFKA_CLIENT_ID, "drop")

    val zooQuorum = hbaseConfiguration.get(HConstants.ZOOKEEPER_QUORUM)
    val zooPort = hbaseConfiguration.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181)
    conf.set(HBASE_CONF_QUORUM, zooQuorum)
    conf.setInt(HBASE_CONF_QUORUM_PORT, zooPort)
    conf.set(UNIQUE_ID_TABLE_NAME, uidsTableName)
    conf.setInt(UNIQUE_ID_CACHE_SIZE, 100000)

    val hTable = new HTable(hbaseConfiguration, pointsTableName)
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

    job.waitForCompletion(true)
  }

}
