package popeye.rollup

import java.io.File

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{TableName, HBaseTestingUtility}
import org.scalatest.{Inspectors, BeforeAndAfter, Matchers, FlatSpec}
import popeye.{Point, Logging}
import popeye.rollup.RollupMapperEngine.RollupStrategy
import popeye.storage.ValueNameFilterCondition
import popeye.storage.hbase.TsdbFormat.EnabledDownsampling
import popeye.storage.hbase._
import popeye.test.PopeyeTestUtils
import popeye.util.ZkConnect
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import scala.util.Try

class RollupJobIntegrationTest extends FlatSpec with Matchers with Inspectors with BeforeAndAfter with Logging {

  val pointsTableName = "tsdb"
  val uidTableName = "tsdb-uid"
  val metricName = "test"

  var hbaseTestingUtility: HBaseTestingUtility = null
  var actorSystem: ActorSystem = null
  implicit var executionContext: ExecutionContext = null
  var pointsStorage: HBaseStorageConfigured = null
  var pointsStorageConfig: HBaseStorageConfig = null

  val shardAttributeName: String = "cluster"
  before {
    hbaseTestingUtility = HBaseTestingUtility.createLocalHTU()
    hbaseTestingUtility.startMiniCluster()
    info("hbase minicluster started")
    val hbaseZk = hbaseTestingUtility.getZkCluster
    val hbaseZkConnect = ZkConnect(Seq("localhost" -> Some(hbaseZk.getClientPort)), None)
    actorSystem = ActorSystem("popeye")
    executionContext = actorSystem.dispatcher
    val tsdbFormatConfig = TsdbFormatConfig(Seq(StartTimeAndPeriod("05/11/14", 26 /*one year*/)), Set(shardAttributeName))
    pointsStorageConfig = HBaseStorageConfig(
      hbaseConfig = ConfigFactory.empty(),
      hbaseTestingUtility.getConfiguration,
      uidsTableName = uidTableName,
      pointsTableName = pointsTableName,
      pointsTableCoprocessorJarPathOption = None,
      poolSize = 10,
      zkQuorum = hbaseZkConnect,
      readChunkSize = 100,
      resolveTimeout = 10 seconds,
      uidsCacheInitialCapacity = 10000,
      uidsCacheMaxCapacity = 10000,
      tsdbFormatConfig = tsdbFormatConfig,
      storageName = "hbase"
    )
    val metricRegistry = new MetricRegistry()
    pointsStorage = new HBaseStorageConfigured(pointsStorageConfig, actorSystem, metricRegistry)
  }

  after {
    Try(actorSystem.shutdown())
    val hbaseShutdown = Try(hbaseTestingUtility.shutdownMiniCluster())
    info(s"hbase minicluster shutdown: $hbaseShutdown")
    val hbaseDataDeletion = Try {
      hbaseTestingUtility.getDataTestDirOnTestFS()
      hbaseTestingUtility.cleanupTestDir()
    }
    info(s"hbase minicluster data deleted: $hbaseDataDeletion")
    actorSystem.awaitTermination(1 minute)
  }

  behavior of "rollup"

  it should "just work" in {
    val endTime = 1421280000
    val oneWeek = 3600 * 24 * 7
    val oneMonthBeforeEndTime = endTime - oneWeek * 4
    val twoWeeksBeforeEndTime = endTime - oneWeek * 2
    val timestamps = oneMonthBeforeEndTime to endTime by 60
    val points = createTestPoints(timestamps)
    val writeFuture = pointsStorage.storage.writePoints(points)
    Await.result(writeFuture, Duration.Inf)
    val rollupJobRunner = new RollupJobRunner(
      hbaseTestingUtility.getConfiguration,
      TableName.valueOf(pointsTableName),
      hbaseTestingUtility.getConfiguration,
      new Path("/tmp/hbase_restore"),
      new Path("/tmp_bulkload"),
      Seq(),
      pointsStorageConfig.tsdbFormatConfig
    )
    rollupJobRunner.doRollup(0, RollupStrategy.HourRollup, oneMonthBeforeEndTime, twoWeeksBeforeEndTime)
    val attributes = Map(shardAttributeName -> ValueNameFilterCondition.SingleValueName("test"))

    {
      val iter = pointsStorage.storage.getPoints(
        metricName,
        (oneMonthBeforeEndTime, twoWeeksBeforeEndTime),
        attributes,
        EnabledDownsampling(TsdbFormat.DownsamplingResolution.Hour, TsdbFormat.AggregationType.Max)
      )
      val seriesFuture = HBaseStorage.collectSeries(iter)
      val series = Await.result(seriesFuture, Duration.Inf).seriesMap.values.head
      val expectedSeries = (oneMonthBeforeEndTime until twoWeeksBeforeEndTime by 3600).map(ts => Point(ts, ts + 3540))
      val pointsDifference = subtractPoints(series.iterator.toList, expectedSeries)
      val timestampDiffs = pointsDifference.map(_.timestamp)
      all(timestampDiffs) should equal(0)
      val valueDiffs = pointsDifference.map(_.value)
      all(valueDiffs) should be < 0.001
    }

    {
      val iter = pointsStorage.storage.getPoints(
        metricName,
        (twoWeeksBeforeEndTime, endTime),
        attributes,
        EnabledDownsampling(TsdbFormat.DownsamplingResolution.Hour, TsdbFormat.AggregationType.Max)
      )
      val seriesFuture = HBaseStorage.collectSeries(iter)
      Await.result(seriesFuture, Duration.Inf).seriesMap.values should be(empty)
    }

  }

  def subtractPoints(left: Seq[Point], right: Seq[Point]): Seq[Point] = {
    left.size should equal(right.size)
    for ((leftPoint, rightPoint) <- left zip right) yield {
      Point(
        leftPoint.timestamp - rightPoint.timestamp,
        (leftPoint.value - rightPoint.value) / (math.abs(leftPoint.value) + math.abs(rightPoint.value))
      )
    }
  }

  def createTestPoints(timestamps: Seq[Int]) = {
    timestamps.map {
      timestamp =>
        PopeyeTestUtils.createPoint(metricName, timestamp, Seq(shardAttributeName -> "test"), Left(timestamp))
    }
  }


}
