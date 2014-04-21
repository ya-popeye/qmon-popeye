package popeye.hadoop.bulkload

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HConstants, KeyValue}
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat2}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.Configuration
import popeye.util.KafkaUtils
import popeye.javaapi.kafka.hadoop.KafkaInput
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import org.apache.hadoop.mapred.JobConf
import popeye.hadoop.bulkload.BulkLoadConstants._
import scala.Some
import popeye.storage.hbase.CreateTsdbTables
import popeye.Logging

object BulkloadJobRunner extends Logging {

  private val zkTimeout: Int = 5000

  def doBulkload(jobRunnerZkConnect: String,
                 offsetsPath: String,
                 kafkaZkConnect: String,
                 brokerList: String,
                 topic: String,
                 hbaseConfiguration: Configuration,
                 tableName: String,
                 hadoopConfiguration: Configuration,
                 outputPath: Path) {

    val previousStopOffsets = loadPreviousStopOffsets(jobRunnerZkConnect, offsetsPath)
    info(f"kafka offsets loaded: $previousStopOffsets, creating kafka inputs for ($kafkaZkConnect, $topic, $previousStopOffsets)")
    val kafkaInputs = getKafkaInputs(kafkaZkConnect, topic, previousStopOffsets)
    info(f"kafka inputs created: $kafkaInputs, staring hadoop job")
    runHadoopJob(hadoopConfiguration, hbaseConfiguration, brokerList, kafkaInputs, outputPath)
    info("hadoop job finished, bulkload starting")
    val hTable = new HTable(hbaseConfiguration, tableName)
    try {
      new LoadIncrementalHFiles(hbaseConfiguration).doBulkLoad(outputPath, hTable)
    } finally {
      hTable.close()
    }
    info(f"bulkload finished, saving offsets")
    val stopOffsets =
      kafkaInputs.map(input => input.partition -> input.stopOffset)
        .toMap.withDefault(previousStopOffsets)
    saveOffsets(jobRunnerZkConnect, offsetsPath, stopOffsets)
    FileSystem.get(hadoopConfiguration).delete(outputPath, true)
    info(f"offsets saved")
  }


  private def getKafkaInputs(kafkaZkConnect: String, topic: String, previousStopOffsets: Map[Int, Long]) = {
    val partitionMeta = KafkaUtils.fetchPartitionsMetadata(kafkaZkConnect, topic)
    val partitionIds = partitionMeta.filter(_.leader.isDefined).map(_.partitionId)
    val brokers = partitionMeta.map(_.leader).collect {
      case Some(leader) => (leader.host, leader.port)
    }
    val offsets = KafkaUtils.fetchLatestOffsets(brokers, topic, partitionIds)
    offsets.toList.map {
      case (partitionId, offset) =>
        KafkaInput(
          topic,
          partitionId,
          startOffset = previousStopOffsets.getOrElse(partitionId, 0),
          stopOffset = offset
        )
    }
  }

  private def saveOffsets(rootZkConnect: String, offsetsPath: String, offsets: Map[Int, Long]) = {
    val zkClient = new ZkClient(rootZkConnect, zkTimeout, zkTimeout, ZKStringSerializer)
    try {
      val offsetsString = offsets.toList.map {
        case (partition, offset) => f"$partition:$offset"
      }.mkString(",")
      zkClient.writeData(offsetsPath, offsetsString)
    } finally {
      zkClient.close()
    }
  }

  private def loadPreviousStopOffsets(rootZkConnect: String, offsetsPath: String): Map[Int, Long] = {
    val zkClient = new ZkClient(rootZkConnect, zkTimeout, zkTimeout, ZKStringSerializer)
    try {
      if (!zkClient.exists(offsetsPath)) {
        val createParents = true
        zkClient.createPersistent(offsetsPath, createParents)
        zkClient.writeData(offsetsPath, "")
      }
      val offsetsString: String = zkClient.readData(offsetsPath)
      offsetsString.split(",").filter(_.nonEmpty).map {
        partitionAndOffset =>
          val tokens = partitionAndOffset.split(":")
          (tokens(0).toInt, tokens(1).toLong)
      }.toMap
    } finally {
      zkClient.close()
    }
  }

  private def runHadoopJob(hadoopConfiguration: Configuration,
                           hbaseConfiguration: Configuration,
                           brokersListSting: String,
                           kafkaInputs: Seq[KafkaInput],
                           outputPath: Path) = {
    val conf: JobConf = new JobConf(hadoopConfiguration)
    conf.set(KAFKA_INPUTS, KafkaInput.renderInputsString(kafkaInputs))
    conf.set(KAFKA_BROKERS, brokersListSting)
    conf.setInt(KAFKA_CONSUMER_TIMEOUT, 5000)
    conf.setInt(KAFKA_CONSUMER_BUFFER_SIZE, 100000)
    conf.setInt(KAFKA_CONSUMER_FETCH_SIZE, 2000000)
    conf.set(KAFKA_CLIENT_ID, "drop")

    val zooQuorum = hbaseConfiguration.get(HConstants.ZOOKEEPER_QUORUM)
    val zooPort = hbaseConfiguration.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181)
    conf.set(HBASE_CONF_QUORUM, zooQuorum)
    conf.setInt(HBASE_CONF_QUORUM_PORT, zooPort)
    conf.set(UNIQUE_ID_TABLE_NAME, CreateTsdbTables.tsdbUidTable.getTableName.getNameAsString)
    conf.setInt(UNIQUE_ID_CACHE_SIZE, 100000)

    val hTable = new HTable(hbaseConfiguration, CreateTsdbTables.tsdbTable.getTableName)
    val job: Job = Job.getInstance(conf)

    job.setJarByClass(BulkloadJobRunner.getClass)
    job.setInputFormatClass(classOf[PopeyePointsKafkaTopicInputFormat])
    job.setMapperClass(classOf[PointsToKeyValueMapper])
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, hTable)
    FileOutputFormat.setOutputPath(job, outputPath)

    job.waitForCompletion(true)
  }

}
