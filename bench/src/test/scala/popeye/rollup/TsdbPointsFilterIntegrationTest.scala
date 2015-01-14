package popeye.rollup

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.{HBaseTestingUtility, KeyValueUtil}
import org.scalatest.{Matchers, FlatSpec}
import popeye.Logging
import popeye.clients.TsPoint
import popeye.inttesting.TestDataUtils
import popeye.proto.Message
import popeye.storage.hbase._
import popeye.util.ZkConnect

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class TsdbPointsFilterIntegrationTest extends FlatSpec with Matchers with Logging {

  val shardAttributeNames: Set[String] = Set("cluster")
  val pointsTableName: String = "tsdb"
  val uIdsTableName: String = "tsdb-uid"


  def createStorageConfig(hBaseZkConnect: ZkConnect) = {
    HBaseStorageConfig(
      hbaseConfig = ConfigFactory.empty(),
      hadoopConfigurationInner = new Configuration(),
      uidsTableName = uIdsTableName,
      pointsTableName = pointsTableName,
      pointsTableCoprocessorJarPathOption = None,
      poolSize = 10,
      zkQuorum = hBaseZkConnect,
      readChunkSize = 10,
      resolveTimeout = 10 seconds,
      uidsCacheInitialCapacity = 1000,
      uidsCacheMaxCapacity = 10000,
      tsdbFormatConfig = TsdbFormatConfig(Seq(StartTimeAndPeriod("25/06/14", 336)), shardAttributeNames),
      storageName = "hbase"
    )
  }

  "TsdbPointsFilter" should "filter by timestamp" in {
    val hbaseTestingUtility = HBaseTestingUtility.createLocalHTU()
    info("creating zookeeper minicluster")
    val miniZkCluster = hbaseTestingUtility.startMiniZKCluster()
    info("creating hbase minicluster")
    val hbaseMiniCluster = hbaseTestingUtility.startMiniCluster()
    val hBaseZkConnect: ZkConnect = ZkConnect.parseString(f"localhost:${miniZkCluster.getClientPort}")
    val storageConfig = createStorageConfig(hBaseZkConnect)
    val actorSystem = ActorSystem()
    implicit val exct = actorSystem.dispatcher
    val storage = new HBaseStorageConfigured(storageConfig, actorSystem, new MetricRegistry())
    val endTime = 1419865200
    val points = createTestPoints(endTime)
    val writeFuture = storage.storage.writeMessagePoints(points: _*)
    Await.result(writeFuture, 5 seconds)

    val pointsFilter = new TsdbPointsFilter(
      13,
      TsdbFormat.ValueTypes.SingleValueTypeStructureId,
      endTime - 3600,
      endTime
    )
    val scan = new Scan()
    scan.setFilter(pointsFilter)
    val readPoints = storage.hTablePool.getTable(pointsTableName).getScanner(scan).asScala
      .flatMap(_.listCells().asScala).map(KeyValueUtil.ensureKeyValue).map(storage.storage.keyValueToPoint)
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    hbaseTestingUtility.shutdownMiniCluster()
    readPoints.foreach {
      point =>
        val timestamp = point.getTimestamp
        val tags = point.getAttributesList.asScala.map(attr => (attr.getName, attr.getValue)).sorted
        info(f"$timestamp $tags")
    }
    readPoints.size should equal(60)
    val timestamps = readPoints.map(_.getTimestamp)
    timestamps.max should equal(endTime - 60)
    timestamps.min should equal(endTime - 3600)

    info(f"total points count: ${points.size}")
  }

  def createTestPoints(endTime: Int) = {
    val timestamps = (endTime - 3600 * 60) until endTime by 60
    //    val periods = Seq(3600 * 10, 3600 * 50, 3600 * 100)
    //    val amps = Seq(1000, 2000, 4000)
    //    val shifts = Seq(3600 * 5, 3600 * 50)
    val periods = Seq(3600 * 10)
    val amps = Seq(1000)
    val shifts = Seq(3600 * 5)
    val points = TestDataUtils.createSinTsPoints("sin", timestamps, periods, amps, shifts, "cluster" -> "test")
    points.map {
      case TsPoint(metricName, timestamp, value, attrs) =>
        val builder = Message.Point.newBuilder()
        builder.setTimestamp(timestamp)
        builder.setMetric(metricName)
        value.fold(
          longValue => {
            builder.setIntValue(longValue)
            builder.setValueType(Message.Point.ValueType.INT)
          },
          floatValue => {
            builder.setFloatValue(floatValue)
            builder.setValueType(Message.Point.ValueType.FLOAT)
          }
        )
        for ((name, value) <- attrs) {
          builder.addAttributesBuilder()
            .setName(name)
            .setValue(value)
        }
        builder.build()
    }
  }
}
