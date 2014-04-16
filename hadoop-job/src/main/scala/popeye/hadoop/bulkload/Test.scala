package popeye.hadoop.bulkload

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.apache.hadoop.hbase.client.HTable
import popeye.javaapi.kafka.hadoop.KafkaInput
import org.apache.hadoop.mapred.JobConf
import popeye.hadoop.bulkload.BulkLoadConstants._
import java.io.File
import org.apache.hadoop.fs.Path

object Test {
  def main(args: Array[String]) {
    new File("/tmp/hadoop/output").delete()
    val kafkaInputsString = {
      val topic = "popeye-points"
      val inputs = for (partition <- 0 to 9) yield {
        KafkaInput(topic, partition, 0, 500)
      }
      KafkaInput.renderInputsString(inputs)
    }

    val brokersString = "localhost:9091,localhost:9092"

    val conf: JobConf = new JobConf
    conf.set(KAFKA_INPUTS, kafkaInputsString)
    conf.set(KAFKA_BROKERS, brokersString)
    conf.setInt(KAFKA_CONSUMER_TIMEOUT, 5000)
    conf.setInt(KAFKA_CONSUMER_BUFFER_SIZE, 100000)
    conf.setInt(KAFKA_CONSUMER_FETCH_SIZE, 2000000)
    conf.set(KAFKA_CLIENT_ID, "drop")

    conf.set(HBASE_CONF_QUORUM, "localhost")
    conf.setInt(HBASE_CONF_QUORUM_PORT, 2182)
    conf.set(UNIQUE_ID_TABLE_NAME, "popeye:tsdb-uid")
    conf.setInt(UNIQUE_ID_CACHE_SIZE, 100000)

    val job: Job = Job.getInstance(conf)
    val hbaseConfiguration: Configuration = HBaseConfiguration.create
    hbaseConfiguration.set("hbase.zookeeper.quorum", "localhost")
    hbaseConfiguration.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2182)
    val table: HTable = new HTable(hbaseConfiguration, "popeye:tsdb")
    val outputPath: Path = new Path("file:////tmp/hadoop/output")
    BulkloadJobRunner.runJob(job, table, outputPath)
  }
}
