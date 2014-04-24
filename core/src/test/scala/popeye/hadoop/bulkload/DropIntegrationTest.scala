package popeye.hadoop.bulkload

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{Config, ConfigFactory}
import java.util.Properties
import kafka.admin.AdminUtils
import kafka.producer.{ProducerConfig, Producer}
import kafka.utils.ZKStringSerializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Scan, Result, ResultScanner, HTable}
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.I0Itec.zkclient.ZkClient
import popeye.pipeline.kafka.{KeySerialiser, KeyPartitioner}
import popeye.proto.Message.Point
import popeye.proto.{PackedPoints, Message}
import popeye.storage.hbase.{HBaseStorageConfigured, HBaseStorageConfig, CreateTsdbTables}
import scala.collection.JavaConverters._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import kafka.producer.KeyedMessage
import java.io.{PrintStream, File, StringReader}
import popeye.util.{OffsetRange, KafkaOffsetsTracker, KafkaMetaRequests}
import popeye.javaapi.kafka.hadoop.KafkaInput

object DropIntegrationTest {
  val pointsTableName: String = "popeye:tdsb"
  val uIdsTableName: String = "popeye:tsdb-uid"

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
    CreateTsdbTables.createTables(hbaseConfiguration, pointsTableName, uIdsTableName)
  }

  def createKafkaTopic(zkConnect: String, topic: String, partitions: Int) = {
    val zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
    AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor = 1)
  }

  def loadPointsFromHBase(hbaseConfiguration: Configuration, storageConfig: Config) = {

    val actorSystem = ActorSystem()
    val configuredStorage = new HBaseStorageConfigured(new HBaseStorageConfig(
      storageConfig,
      actorSystem,
      new MetricRegistry()
    ))
    val tsdbTable = new HTable(hbaseConfiguration, pointsTableName)
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
    val random = new Random(0)
    val tagLists = for {
      host <- 0 to 100
    } yield {
      Seq(("host", host.toString), ("cluster", (host % 10).toString), ("dc", (host % 100).toString))
    }
    for {
      metric <- Seq("foo", "bar", "baz")
      tagList <- tagLists
      timestamp <- Seq.fill(1000)(random.nextInt(100000)).distinct
    } yield {
      val attributes = tagList.map {
        case (name, value) => Message.Attribute.newBuilder().setName(name).setValue(value).build()
      }
      val builder = Message.Point.newBuilder()
        .setMetric(metric)
        .setTimestamp(timestamp)
        .addAllAttributes(attributes.asJava)
      val pointValue = randomValue(random)
      pointValue.fold(
        longValue => builder.setIntValue(longValue),
        floatValue => builder.setFloatValue(floatValue)
      )
      builder.build()
    }
  }

  def randomValue(rnd: Random): Either[Long, Float] = {
    if (rnd.nextBoolean()) {
      Left(rnd.nextLong())
    } else {
      Right(rnd.nextFloat())
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

    val hadoopConfiguration = {
      val conf = new Configuration()
      for (path <- Seq("/home/quasi/programming/sandbox/hadoop/hadoop-2.3.0-cdh5.0.0/etc/hadoop/mapred-site.xml",
        "/home/quasi/programming/sandbox/hadoop/hadoop-2.3.0-cdh5.0.0/etc/hadoop/core-site.xml")) {
        conf.addResource(new File(path).toURI.toURL)
      }
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
    createKafkaTopic(kafkaZkConnect, topic, partitions)
    Thread.sleep(1000)
    loadPointsToKafka(brokersListSting, topic, points)
    createTsdbTables(hbaseConfiguration)
    val kafkaBrokers = Seq("localhost" -> 9091, "localhost" -> 9092)
    val kafkaMetaRequests = new KafkaMetaRequests(kafkaBrokers, topic)
    val offsetsTracker = new KafkaOffsetsTracker(kafkaMetaRequests, "localhost:2181", "/offsets")
    val offsetRanges = offsetsTracker.fetchOffsetRanges()
    val kafkaInputs = offsetRanges.toList.map {
      case (partitionId, OffsetRange(startOffset, stopOffset)) =>
        KafkaInput(
          topic,
          partitionId,
          startOffset,
          stopOffset
        )
    }.filterNot(_.isEmpty)
    if (kafkaInputs.nonEmpty) {
      BulkloadJobRunner.doBulkload(
        kafkaInputs,
        kafkaBrokers,
        hbaseConfiguration,
        pointsTableName = pointsTableName,
        uIdsTableName = uIdsTableName,
        hadoopConfiguration,
        jarsHdfsPath = "/popeye/lib",
        outputHdfsPath = "/bulkload/output"
      )
    }
    offsetsTracker.commitOffsets(offsetRanges)

    val loadedPoints = loadPointsFromHBase(hbaseConfiguration, storageConfig)

    if (arePointsEqual(points, loadedPoints)) {
      println("OK")
    } else {
      println(points.size)
      println(loadedPoints.size)
      val hashable = points.map(new HashablePoint(_))
      val loadedHashable = loadedPoints.map(new HashablePoint(_))
      val out = new PrintStream(new File("DropIntegrationTest.dump"))
      for ((orig, loaded) <- hashable.sortBy(_.toString) zip loadedHashable.sortBy(_.toString)) {
        out.println(f"$orig!$loaded")
      }
      out.close()
    }

    //    BulkloadJobRunner.doBulkload(
    //      jobRunnerZkConnect = "localhost:2181",
    //      offsetsPath = "/offsets",
    //      kafkaZkConnect = "localhost:2181",
    //      brokerList = "localhost:9091,localhost:9092",
    //      topic,
    //      hbaseConfiguration,
    //      pointsTableName = CreateTsdbTables.tsdbTable.getTableName.getNameAsString,
    //      uIdsTableName = CreateTsdbTables.tsdbUidTable.getTableName.getNameAsString,
    //      hadoopConfiguration = new Configuration(),
    //      jobOutputPath
    //    )
  }
}
