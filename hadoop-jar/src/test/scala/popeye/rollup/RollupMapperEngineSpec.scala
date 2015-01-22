package popeye.rollup

import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.{CellUtil, KeyValue}
import org.scalatest.Matchers
import popeye.Point
import popeye.hadoop.bulkload.LightweightUniqueId
import popeye.rollup.RollupMapperEngine.RollupStrategy
import popeye.storage.ValueNameFilterCondition.SingleValueName
import popeye.storage.hbase.TsdbFormat.EnabledDownsampling
import popeye.storage.hbase._
import popeye.test.{AkkaTestKitSpec, PopeyeTestUtils}

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

class RollupMapperEngineSpec extends AkkaTestKitSpec("points-storage") with Matchers {

  implicit val ecxt = system.dispatcher
  behavior of "RollupMapperEngine"

  it should "do a rollup" in {
    val storageStub = new PointsStorageStub()
    val numberOfPoints = 3600
    val step = 60
    val startTime = 0
    val points = (0 until numberOfPoints).map {
      i =>
        PopeyeTestUtils.createPoint(
          metric = "test",
          timestamp = startTime + i * step,
          attributes = Seq("host" -> "yandex.net"),
          value = Left((i * step) / 3600)
        )
    }
    val writeFuture = storageStub.storage.writePoints(points)
    Await.result(writeFuture, Duration.Inf)

    val mapperEngine = new RollupMapperEngine(storageStub.tsdbFormat, RollupStrategy.HourRollup, 0)
    val results = storageStub.pointsTable.getScanner(new Scan).asScala.toBuffer
    val keyValues = mutable.Buffer[KeyValue]()
    for (result <- results) {
      keyValues ++= mapperEngine.map(result).asScala.map(_.keyValue)
    }
    keyValues ++= mapperEngine.cleanup().asScala.map(_.keyValue)
    val puts = keyValues.map(kv => new Put(CellUtil.cloneRow(kv)).add(kv)).asJava
    storageStub.pointsTable.put(puts)
    val pointsAsyncIter = storageStub.storage.getPoints(
      "test",
      (0, 3600 * 60),
      Map("host" -> SingleValueName("yandex.net")),
      EnabledDownsampling(TsdbFormat.DownsamplingResolution.Hour, TsdbFormat.AggregationType.Avg)
    )
    val seriesFuture = HBaseStorage.collectSeries(pointsAsyncIter, Promise().future)
    val pointSeries = Await.result(seriesFuture, Duration.Inf)
    val allAggregatedPoints = pointSeries.seriesMap(SortedMap("host" -> "yandex.net")).iterator.toList
    val expectedPoints = (0 until numberOfPoints / (3600 / step)).map(i => Point(startTime + i * 3600, i))
    allAggregatedPoints should equal(expectedPoints)
  }
}
