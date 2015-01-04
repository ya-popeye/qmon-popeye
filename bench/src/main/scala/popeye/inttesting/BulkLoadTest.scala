package popeye.inttesting

import java.io.{File, StringReader}
import java.util.Properties

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{Config, ConfigFactory}
import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.{HTable, Result, ResultScanner, Scan}
import org.apache.zookeeper.CreateMode
import popeye.Logging
import popeye.hadoop.bulkload.{BulkLoadJobRunner, BulkLoadMetrics}
import popeye.pipeline.kafka.{KeyPartitioner, KeySerialiser}
import popeye.proto.Message.Point
import popeye.proto.{Message, PackedPoints}
import popeye.storage.hbase._
import popeye.util.{ZkClientConfiguration, ZkConnect}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

object BulkLoadTest extends Logging {
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

  val shardAttributeNames: Set[String] = Set("dc")

  def loadPointsFromHBase(storageConfig: Config) = {
    val actorSystem = ActorSystem()
    val baseStorageConfig: HBaseStorageConfig = HBaseStorageConfig(storageConfig, shardAttributeNames)
    info("creating HBaseStorageConfigured")
    val configuredStorage = new HBaseStorageConfigured(baseStorageConfig, actorSystem, new MetricRegistry())
    info("HBaseStorageConfigured was created")
    info("creating HTable")
    val tsdbTable = configuredStorage.hTablePool.getTable(pointsTableName)
    info("HTable was created")
    val chunkSize = 1000
    info("creating scanner")
    val scanner = tsdbTable.getScanner(new Scan())
    info("scanner scanner was created")
    //    val tColumn = CreateTsdbTables.tsdbTable.getFamilies.iterator().next()
    val results = asResultIterator(scanner).grouped(chunkSize).map {
      kvs =>
        info(f"loaded $chunkSize results")
        kvs
    }.toList.flatten
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
      f"Point($metric, $timestamp, $attributes, ${ value.fold(_.toString + "l", _.toString + "f") })"
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
        longValue => {
          builder.setValueType(Message.Point.ValueType.INT)
          builder.setIntValue(longValue)
        },
        floatValue => {
          builder.setValueType(Message.Point.ValueType.FLOAT)
          builder.setFloatValue(floatValue)
        }
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
    val jarsPath = args(0)
    val outputPath = args(1)
    val points = createPoints

    val kafkaBrokerPort = 9091
    val brokersListSting = f"localhost:$kafkaBrokerPort"

    val topic = "popeye-points-drop"
    val hbaseTestingUtility = HBaseTestingUtility.createLocalHTU()
    info("creating zookeeper minicluster")
    val miniZkCluster = hbaseTestingUtility.startMiniZKCluster()
    info("creating hbase minicluster")
    val hbaseMiniCluster = hbaseTestingUtility.startMiniCluster()

    val rootZkConnect = ZkConnect.parseString(f"localhost:${ miniZkCluster.getClientPort }")
    createChroots(rootZkConnect, Seq("/popeye", "/kafka"))
    val popeyeZkConnect = rootZkConnect.withChroot("/popeye")
    val kafkaZkConnect = rootZkConnect.withChroot("/kafka")

    val kafka = EmbeddedKafka.create(
      logsDir = new File("/tmp/bulkload_test"),
      zkConnect = kafkaZkConnect,
      port = kafkaBrokerPort
    )
    kafka.start()

    val hbaseConfiguration = hbaseTestingUtility.getConfiguration
    val hadoopConfiguration = new Configuration()

    val storageConfig = ConfigFactory.parseString(
      f"""
        |zk.quorum = "${ rootZkConnect.toZkConnectString }"
        |pool.max = 25
        |read-chunk-size = 10
        |table {
        |  points = "$pointsTableName"
        |  uids = "$uIdsTableName"
        |}
        |
        |uids {
        |  resolve-timeout = 10s
        |  cache { initial-capacity = 1000, max-capacity = 100000 }
        |}
        |
        |generations = [
        |  {
        |    // date format: dd/MM/yy
        |    start-date = "16/06/14"
        |    rotation-period-hours = 168 // 24 * 7
        |  },
        |  {
        |    start-date = "01/09/14"
        |    rotation-period-hours = 24
        |  }
        |]
      """.stripMargin)
    kafka.createTopic(topic, partitions = 10)
    Thread.sleep(1000)
    info("sending points to kafka")
    loadPointsToKafka(brokersListSting, topic, points)
    info("points was sent")
    createTsdbTables(hbaseConfiguration)
    info("tsdb tables was created")
    val kafkaBrokers = Seq("localhost" -> 9091, "localhost" -> 9092)
    val tsdbFormatConfig = {
      val startTimeAndPeriods = StartTimeAndPeriod.parseConfigList(storageConfig.getConfigList("generations"))
      TsdbFormatConfig(startTimeAndPeriods, shardAttributeNames)
    }
    val hbaseConfig = BulkLoadJobRunner.HBaseStorageConfig(
      hBaseZkConnect = rootZkConnect,
      pointsTableName = pointsTableName,
      uidTableName = uIdsTableName,
      tsdbFormatConfig = tsdbFormatConfig
    )

    val bulkloadRunnerConfig = BulkLoadJobRunner.JobRunnerConfig(
      kafkaBrokers = kafkaBrokers,
      topic = topic,
      outputPath = outputPath,
      jarsPath = jarsPath,
      zkClientConfig = ZkClientConfiguration(popeyeZkConnect, 1000, 1000),
      hadoopConfiguration = hadoopConfiguration
    )

    val bulkLoadJobRunner = new BulkLoadJobRunner(
      name = "test",
      storageConfig = hbaseConfig,
      runnerConfig = bulkloadRunnerConfig,
      metrics = new BulkLoadMetrics("bulkload", new MetricRegistry)
    )

    info("starting bulkload")
    bulkLoadJobRunner.doBulkload()
    info("bulkload finished")

    info("loading points from hbase")
    val loadedPoints = loadPointsFromHBase(storageConfig)
    info(f"${ loadedPoints.size } points was loaded")

    if (arePointsEqual(points, loadedPoints)) {
      info("OK")
    } else {
      info(f"points count: ${ points.size }")
      val hashable = points.map(new HashablePoint(_))
      val loadedHashable = loadedPoints.map(new HashablePoint(_))
      for ((orig, loaded) <- hashable.sortBy(_.toString) zip loadedHashable.sortBy(_.toString)) {
        info(f"$orig!$loaded")
      }
    }
    info("shutting down kafka")
    kafka.shutdown()
    info("shutting down hbase minicluster")
    hbaseTestingUtility.shutdownMiniCluster()
  }

  def createChroots(zkConnect: ZkConnect, roots: Seq[String]) = {
    val zkClient = new ZkClient(zkConnect.serversString)
    for (root <- roots) {
      zkClient.create(s"$root", "", CreateMode.PERSISTENT)
    }
    zkClient.close()
  }
}
