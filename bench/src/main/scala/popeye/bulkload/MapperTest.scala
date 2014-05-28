package popeye.bulkload

import popeye.hadoop.bulkload.{LightweightUniqueId, KafkaPointsIterator}
import popeye.storage.hbase.UniqueIdStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.apache.hadoop.hbase.client.HTablePool
import kafka.consumer.SimpleConsumer
import popeye.javaapi.kafka.hadoop.KafkaInput
import popeye.javaapi.hadoop.bulkload.TsdbKeyValueIterator
import com.codahale.metrics.{CsvReporter, ConsoleReporter, MetricRegistry}
import java.util.concurrent.TimeUnit
import java.io.File

object MapperTest {
  def main(args: Array[String]) {
    val metrics = new MetricRegistry

    val argsMap = parseArgs(args)

    val metricsDir = argsMap("metricsDir")

    val uIdTableName = "popeye:tsdb-uid"
    val kafkaInput = parseKafkaInput(argsMap("kafkaInput"))
    //    val hbaseConf = parseHBaseConf(argsMap("hbase"))
    val Array(kafkaHost, kafkaPort) = argsMap("kafkaBroker").split(":")
    val kafkaConsumerBufferSize = (argsMap("consumerBuffer").toDouble * 1024 * 1024).toInt
    val fetchSize = (argsMap("fetchSize").toDouble * 1024 * 1024).toInt

    val consoleReporter = ConsoleReporter
      .forRegistry(metrics)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build()
    consoleReporter.start(1, TimeUnit.SECONDS)

    val csvReporter = CsvReporter
      .forRegistry(metrics)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(new File(metricsDir))

    csvReporter.start(5, TimeUnit.SECONDS)

    val kafkaConsumer = new SimpleConsumer(kafkaHost, kafkaPort.toInt, soTimeout = 5000, kafkaConsumerBufferSize, clientId = "test")
    val pointsIterator = new KafkaPointsIterator(kafkaConsumer, kafkaInput, fetchSize, clientId = "test")
    //    val hTablePool = new HTablePool(hbaseConf, 10)
    //    val uniqueIdStorage = new UniqueIdStorage(uIdTableName, hTablePool)
    //    val uniqueId = new LightweightUniqueId(uniqueIdStorage, maxCacheSize = 10000)
    //    val keyValueIterator = new TsdbKeyValueIterator(pointsIterator, uniqueId)
    //    import scala.collection.JavaConverters._
    val pointsMeter = metrics.meter("points")
    for (points <- pointsIterator) {
      pointsMeter.mark(points.size)
    }
  }

  def parseKafkaInput(str: String) = {
    val Array(topic, partition, startOffset, endOffset) = str.split(" ")
    KafkaInput(topic, partition.toInt, startOffset.toInt, endOffset.toInt)
  }

  def parseHBaseConf(str: String) = {
    val Array(hosts, port) = str.split(":")
    val hbaseConf: Configuration = HBaseConfiguration.create
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, hosts)
    hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, port.toInt)
    hbaseConf
  }

  def parseArgs(args: Seq[String]): Map[String, String] = {
    args.grouped(2).map {
      pair =>
        require(pair.size == 2, s"key without value ${pair(0)}")
        val key = pair(0)
        require(key.startsWith("--"), "key must have a \'--\' prefix")
        val value = pair(1)
        (key.substring(2), value)
    }.toMap.withDefault {
      key => throw new RuntimeException(f"no value for key $key")
    }
  }
}
