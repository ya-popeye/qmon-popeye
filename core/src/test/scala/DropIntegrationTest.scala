import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{Config, ConfigFactory}
import java.io.StringReader
import java.util.Properties
import kafka.admin.AdminUtils
import kafka.api.{PartitionOffsetRequestInfo, OffsetRequest}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.utils.ZKStringSerializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Scan, Result, ResultScanner, HTable}
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.I0Itec.zkclient.ZkClient
import popeye.hadoop.bulkload.BulkLoadConstants._
import popeye.hadoop.bulkload.BulkloadJobRunner
import popeye.javaapi.kafka.hadoop.KafkaInput
import popeye.pipeline.kafka.{KeySerialiser, KeyPartitioner}
import popeye.proto.Message.Point
import popeye.proto.{PackedPoints, Message}
import popeye.storage.hbase.{HBaseStorageConfigured, HBaseStorageConfig, CreateTsdbTables}
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object DropIntegrationTest {

  def loadPointsToKafka(brokersListSting: String, topic: String, points: Seq[Point]) = {
    val props = new Properties()
    val batchIdRandom = new Random(0)

    val propsString =
      f"""
        |request.required.acks=2
        |request.timeout.ms=100
        |producer.type=sync
        |message.send.max.retries=2
        |retry.backoff.ms=100
        |topic.metadata.refresh.interval.ms=60000
      """.stripMargin
    props.load(new StringReader(propsString))
    props.setProperty("metadata.broker.list", brokersListSting)
    props.setProperty("key.serializer.class", classOf[KeySerialiser].getName)
    props.setProperty("partitioner.class", classOf[KeyPartitioner].getName)
    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[Long, Array[Byte]](producerConfig)
    for (batch <- points.grouped(10)) {
      val packed = PackedPoints(batch)
      producer.send(new KeyedMessage(topic, math.abs(batchIdRandom.nextLong()), packed.copyOfBuffer))
    }
    producer.close()
  }

  def createTsdbTables(hbaseConfiguration: Configuration) = {
    CreateTsdbTables.createTables(hbaseConfiguration)
  }

  def runHadoopJob(hbaseConfiguration: Configuration, brokersListSting: String, kafkaInputs: Seq[KafkaInput], outputPath: Path) = {
    val conf: JobConf = new JobConf
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

    BulkloadJobRunner.runJob(job, hTable, outputPath)
  }

  def createKafkaTopic(zkConnect: String, topic: String, partitions: Int) = {
    val zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
    AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor = 1)
  }

  def fetchKafkaOffsets(brokers: Seq[(String, Int)], topic: String, partitions: Int): Map[Int, Long] = {
    def fetchOffsetsFrom(broker: (String, Int)) = {
      val (host, port) = broker
      val consumer = new SimpleConsumer(host, port, 1000, 10000, "drop")
      try {
        val topicAndPartitions = (0 until partitions).map(i => TopicAndPartition(topic, i))
        val offsetReqInfo = PartitionOffsetRequestInfo(OffsetRequest.LatestTime, maxNumOffsets = 10)
        val offsetsRequest = OffsetRequest(requestInfo = topicAndPartitions.map(tap => tap -> offsetReqInfo).toMap)
        val offsets = consumer.getOffsetsBefore(offsetsRequest)
        println(offsets.partitionErrorAndOffsets)
        val partitionAndOffsets =
          for {
            (tap, offsetResponse) <- offsets.partitionErrorAndOffsets if offsetResponse.error == ErrorMapping.NoError
          }
          yield {
            (tap.partition, offsetResponse.offsets.head)
          }
        partitionAndOffsets.toMap
      } finally {
        consumer.close()
      }
    }
    brokers.map(broker => fetchOffsetsFrom(broker)).reduce(_ ++ _)
  }

  def bulkloadHFiles(hbaseConfiguration: Configuration, hFilesPath: Path, hTable: HTable) = {
    new LoadIncrementalHFiles(hbaseConfiguration).doBulkLoad(hFilesPath, hTable)
  }

  def loadPointsFromHBase(hbaseConfiguration: Configuration, storageConfig: Config) = {

    val actorSystem = ActorSystem()
    val configuredStorage = new HBaseStorageConfigured(new HBaseStorageConfig(
      storageConfig,
      actorSystem,
      new MetricRegistry()
    ))
    val tsdbTable = new HTable(hbaseConfiguration, CreateTsdbTables.tsdbTable.getTableName)
    //    val tColumn = CreateTsdbTables.tsdbTable.getFamilies.iterator().next()
    val results = asResultIterator(tsdbTable.getScanner(new Scan())).toList
    val points = for {
      result <- results if !result.isEmpty
      keyValue <- result.raw()
    } yield {
      configuredStorage.storage.keyValueToPoint(keyValue)
    }
    actorSystem.shutdown()
    points
  }

  private def asResultIterator(scanner: ResultScanner) = new Iterator[Result] {
    private var currentResult = scanner.next()

    override def next(): Result = {
      val oldResult = currentResult
      currentResult = scanner.next()
      oldResult
    }

    override def hasNext: Boolean = currentResult != null
  }

  class HashablePoint(point: Message.Point) {
    val metric = point.getMetric
    val attributes = point.getAttributesList.asScala
      .map(attr => (attr.getName, attr.getValue)).sortBy(_._1).toList
    val timestamp = point.getTimestamp
    val value: Either[Long, Float] =
      if (point.hasFloatValue) {
        Right(point.getFloatValue)
      } else {
        Left(point.getIntValue)
      }

    override def toString: String = {
      f"Point($metric, $timestamp, $attributes, ${value.fold(_.toString + "l", _.toString + "f")})"
    }

    override def equals(obj: scala.Any): Boolean = obj.isInstanceOf[HashablePoint] && obj.toString == toString

    override def hashCode(): Int = toString.hashCode
  }

  def arePointsEqual(points1: Seq[Point], points2: Seq[Point]) = {
    def asSet(points: Seq[Point]) = points.map(new HashablePoint(_)).toSet
    asSet(points1) == asSet(points2)
  }

  def createPoints = {
    val tagLists = for {
      host <- 0 to 100
    } yield {
      Seq(("host", host.toString), ("cluster", (host % 10).toString), ("dc", (host % 100).toString))
    }
    for {
      metric <- Seq("foo", "bar", "baz")
      tagList <- tagLists
    } yield {
      val attributes = tagList.map {
        case (name, value) => Message.Attribute.newBuilder().setName(name).setValue(value).build()
      }
      Message.Point.newBuilder()
        .setMetric(metric)
        .setTimestamp(0)
        .setIntValue(0)
        .addAllAttributes(attributes.asJava)
        .build()
    }
  }

  def main(args: Array[String]) {
    val points = createPoints

    val brokers = Seq(
      ("localhost", 9091),
      ("localhost", 9092)
    )
    val brokersListSting = brokers.map {case (host, port) => f"$host:$port"}.mkString(",")

    val topic = "popeye-points-drop"

    val hbaseConfiguration = {
      val conf = HBaseConfiguration.create
      conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
      conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2182)
      conf
    }

    val storageConfig = ConfigFactory.parseString(
      """
        |zk.quorum = "localhost:2182"
        |pool.max = 25
        |read-chunk-size = 10
        |table {
        |  points = "popeye:tsdb"
        |  uids = "popeye:tsdb-uid"
        |}
        |
        |uids {
        |  resolve-timeout = 10s
        |  metric { initial-capacity = 1000, max-capacity = 100000 }
        |  tagk { initial-capacity = 1000, max-capacity = 100000 }
        |  tagv { initial-capacity = 1000, max-capacity = 100000 }
        |}
      """.stripMargin)

    val partitions = 10
    val kafkaZkConnect = "localhost:2181"
    val jobOutputPath: Path = new Path("file:////tmp/hadoop/output")
    createKafkaTopic(kafkaZkConnect, topic, partitions)
    Thread.sleep(1000)
    loadPointsToKafka(brokersListSting, topic, points)

    val offsets = fetchKafkaOffsets(brokers, topic, partitions)
    val kafkaInputs = (0 until partitions).map {
      partition => KafkaInput(topic, partition, 0, offsets(partition))
    }
    println(offsets)
    createTsdbTables(hbaseConfiguration)
    runHadoopJob(hbaseConfiguration, brokersListSting, kafkaInputs, jobOutputPath)
    val tsdbTable = new HTable(hbaseConfiguration, CreateTsdbTables.tsdbTable.getTableName)
    bulkloadHFiles(hbaseConfiguration, jobOutputPath, tsdbTable)
    val loadedPoints = loadPointsFromHBase(hbaseConfiguration, storageConfig)

    if (arePointsEqual(points, loadedPoints)) {
      println("OK")
    } else {
      println(points.size)
      println(loadedPoints.size)
      val hashable = points.map(new HashablePoint(_))
      val loadedHashable = loadedPoints.map(new HashablePoint(_))
      for ((orig, loaded) <- hashable.sortBy(_.toString) zip loadedHashable.sortBy(_.toString)) {
        println(f"$orig!$loaded")
      }
    }

  }
}
