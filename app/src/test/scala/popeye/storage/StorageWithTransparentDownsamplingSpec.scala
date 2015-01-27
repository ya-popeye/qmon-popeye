package popeye.storage

import java.util.concurrent.Executors

import org.scalatest.{Matchers, FlatSpec}
import popeye.storage.hbase.TsdbFormat
import popeye.storage.hbase.TsdbFormat.AggregationType.AggregationType
import popeye.{PointRope, Point}
import popeye.storage.hbase.TsdbFormat.{EnabledDownsampling, NoDownsampling, Downsampling}
import popeye.storage.hbase.TsdbFormat.DownsamplingResolution._
import popeye.storage.hbase.TsdbFormat.AggregationType._
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.Duration

import scala.concurrent.{Await, Promise, ExecutionContext, Future}

class StorageWithTransparentDownsamplingSpec extends FlatSpec with Matchers {

  implicit val exct = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  behavior of "StorageWithTransparentDownsampling"

  it should "not downsample series" in {
    val stub = storageStub(Map(
      NoDownsampling -> Seq(Point(0, 1), Point(1, 1))
    ))
    getSeries(stub, (0, 2), None) should equal(Seq(Point(0, 1), Point(1, 1)))
  }

  it should "choose 5 minute downsampling" in {
    val stub = storageStub(Map(
      EnabledDownsampling(Minute5, Avg) -> Seq(Point(0, 1), Point(300, 1))
    ))
    getSeries(stub, (0, 400), Some((300, Avg))) should equal(Seq(Point(150, 1), Point(450, 1)))
  }

  it should "choose 5 minute downsampling and then failover to raw data" in {
    val stub = storageStub(Map(
      EnabledDownsampling(Minute5, Avg) -> Seq(Point(0, 1)),
      NoDownsampling -> Seq(Point(450, 2))
    ))
    getSeries(stub, (0, 600), Some((300, Avg))) should equal(Seq(Point(150, 1), Point(450, 2)))
  }

  def getSeries(stub: StorageWithTransparentDownsampling,
                timeRange: (Int, Int),
                downsampling: Option[(Int, AggregationType)]) = {
    val future = stub.getSeries("", timeRange, Map(), downsampling, Promise().future)
    Await.result(future, Duration.Inf).seriesMap(SortedMap.empty).iterator.toList
  }

  def storageStub(series: Map[Downsampling, Seq[Point]]) = {
    val storage = new TimeseriesStorageStub(series)
    new StorageWithTransparentDownsampling(storage)
  }


  class TimeseriesStorageStub(series: Map[Downsampling, Seq[Point]]) extends TimeseriesStorage {
    override def getSeries(metric: String,
                           timeRange: (Int, Int),
                           attributes: Map[String, ValueNameFilterCondition],
                           downsampling: Downsampling,
                           cancellation: Future[Nothing])
                          (implicit eCtx: ExecutionContext): Future[PointsSeriesMap] = {
      val (startTime, stopTime) = timeRange
      val theSeries = series(downsampling).filter(p => p.timestamp >= startTime && p.timestamp < stopTime)
      val rope = PointRope.fromIterator(theSeries.iterator)
      val seriesMap = PointsSeriesMap(Map(SortedMap[String, String]() -> rope))
      Future.successful(seriesMap)
    }
  }

}
