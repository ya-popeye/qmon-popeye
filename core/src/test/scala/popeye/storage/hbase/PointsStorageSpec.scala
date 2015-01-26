package popeye.storage.hbase

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import popeye.storage.hbase.TsdbFormat.{AggregationType, DownsamplingResolution, EnabledDownsampling}
import popeye.storage.{PointAttributes, PointsSeriesMap, QualifiedName}
import popeye.{PointRope, AsyncIterator, Point, ListPoint}
import popeye.test.PopeyeTestUtils._
import popeye.test.{PopeyeTestUtils, MockitoStubs}
import popeye.test.AkkaTestKitSpec
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._
import popeye.proto.Message
import nl.grons.metrics.scala.Meter
import scala.util.Random
import popeye.storage.hbase.HBaseStorage._
import scala.collection.immutable.SortedMap
import popeye.storage.ValueNameFilterCondition._

/**
 * @author Andrey Stepachev
 */
class PointsStorageSpec extends AkkaTestKitSpec("points-storage") with MockitoStubs {

  implicit val executionContext = system.dispatcher

  final val tableName = "my-table"

  behavior of "PointsStorage"

  it should "save and load point correctly" in {
    import scala.collection.JavaConverters._
    val attrNames: Seq[String] = Seq("host", "anotherHost", "additionalHost")
    val storageStub = new PointsStorageStub()
    val random = new Random(0)
    val metric = PopeyeTestUtils.names.head
    val attributes = attrNames.map {
      name =>
        val value = PopeyeTestUtils.hosts(random.nextInt(PopeyeTestUtils.hosts.size))
        (name, value)
    }
    val point = messagePoint(metric, timestamp = 0, 0, attributes)
    Await.ready(storageStub.storage.writeMessagePoints(point), 5 seconds)
    val points = storageStub.pointsTable.getScanner(TsdbFormat.PointsFamily).map(_.raw).flatMap {
      kv =>
        kv.map(storageStub.storage.keyValueToPoint)
    }
    points.size should equal(1)
    val loadedPoint = points.head
    loadedPoint.getMetric should equal(point.getMetric)
    loadedPoint.getAttributesList.asScala.toSet should equal(point.getAttributesList.asScala.toSet)
    loadedPoint.hasFloatValue should equal(point.hasFloatValue)
    loadedPoint.getIntValue should equal(point.getIntValue)
  }

  it should "produce key values" in {
    implicit val random = new java.util.Random(1234)

    val storageStub = new PointsStorageStub()

    val events = mkEvents(msgs = 4)
    val future = storageStub.storage.writeMessagePoints(events :_*)
    val written = Await.result(future, 5 seconds)
    written should be(events.size)
    val points = storageStub.pointsTable.getScanner(TsdbFormat.PointsFamily).map(_.raw).flatMap {
      kv =>
        kv.map(storageStub.storage.keyValueToPoint)
    }
    points.size should be(events.size)
    events.toList.sortBy(_.getTimestamp) should equal(points.toList.sortBy(_.getTimestamp))

    // write once more, we shold write using short path
    val future2 = storageStub.storage.writeMessagePoints(events :_*)
    val written2 = Await.result(future2, 5 seconds)
    written2 should be(events.size)

  }

  ignore should "performance test" in {
    implicit val random = new java.util.Random(1234)

    val storageStub = new PointsStorageStub()

    val events = mkEvents(msgs = 4000)
    for (i <- 1 to 600) {
      val future = storageStub.storage.writeMessagePoints(events :_*)
      val written = Await.result(future, 5 seconds)
      written should be(events.size)
    }
    printMeter(storageStub.pointsStorageMetrics.writeHBasePoints)
  }

  private def printMeter(meter: Meter) {
    printf("             count = %d%n", meter.getCount)
    printf("         mean rate = %2.2f events/s%n", meter.getMeanRate)
    printf("     1-minute rate = %2.2f events/s%n", meter.getOneMinuteRate)
    printf("     5-minute rate = %2.2f events/s%n", meter.getFiveMinuteRate)
    printf("    15-minute rate = %2.2f events/s%n", meter.getFifteenMinuteRate)
  }

  it should "perform time range queries" in {
    val timeRanges = (0 to 3).map(id => (id * 3600, (id + 1) * 3600, id.toShort))
    val timeRangeIdMapping = createGenerationIdMapping(timeRanges: _*)
    val storageStub = new PointsStorageStub(timeRangeIdMapping)
    val points = (0 to 6).map {
      i =>
        messagePoint(
          metricName = "my.metric1",
          timestamp = i * 1200,
          value = i,
          attrs = Seq("host" -> "localhost")
        )
    }
    writePoints(storageStub, points)
    val future = storageStub.storage.getPoints("my.metric1", (1200, 4801), Map("host" -> SingleValueName("localhost")))
    val seriesMap = toSeriesMap(future)
    seriesMap.size should equal(1)
    val series = seriesMap(SortedMap("host" -> "localhost")).iterator.toList
    series should contain(Point(1200, 1))
    series should (not contain Point(0, 0))
    series should (not contain Point(6000, 5))
  }

  it should "not fail if id is not present in some of time ranges" in {
    val timeRangeIdMapping = createGenerationIdMapping((0, 3600, 0), (3600, 7200, 1))
    val storageStub = new PointsStorageStub(timeRangeIdMapping)
    val point = messagePoint(
      metricName = "my.metric1",
      timestamp = 0,
      value = 0,
      attrs = Seq("host" -> "localhost")
    )
    writePoints(storageStub, Seq(point))
    val future = storageStub.storage.getPoints("my.metric1", (0, 4000), Map("host" -> SingleValueName("localhost")))
    val seriesMap = toSeriesMap(future)
    seriesMap.size should equal(1)
    val series = seriesMap(SortedMap("host" -> "localhost")).iterator.toList
    series should contain(Point(0, 0))
  }

  it should "perform multiple attributes queries" in {
    val storageStub = new PointsStorageStub(shardAttrs = Set("a"))
    val point = messagePoint(
      metricName = "metric",
      timestamp = 0,
      value = 1,
      attrs = Seq("a" -> "foo", "b" -> "foo")
    )

    writePoints(storageStub, Seq(point))
    val future = storageStub.storage.getPoints(
      "metric",
      (0, 1),
      Map("a" -> SingleValueName("foo"), "b" -> SingleValueName("foo"))
    )
    val seriesMap = toSeriesMap(future)
    seriesMap.size should equal(1)
    val series = seriesMap(SortedMap("a" -> "foo", "b" -> "foo")).iterator.toList

    series should contain(Point(0, 1))
  }


  it should "perform multiple attribute value queries" in {
    val storageStub = new PointsStorageStub(shardAttrs = Set("type"))
    val fooPoint = messagePoint(
      metricName = "metric",
      timestamp = 0,
      value = 1,
      attrs = Seq("type" -> "foo")
    )
    val barPoint = messagePoint(
      metricName = "metric",
      timestamp = 0,
      value = 2,
      attrs = Seq("type" -> "bar")
    )
    val junkPoint = messagePoint(
      metricName = "metric",
      timestamp = 0,
      value = 3,
      attrs = Seq("type" -> "junk")
    )
    writePoints(storageStub, Seq(fooPoint, barPoint, junkPoint))
    val future = storageStub.storage.getPoints("metric", (0, 1), Map("type" -> MultipleValueNames(Seq("foo", "bar"))))
    val seriesMap = toSeriesMap(future)
    seriesMap.size should equal(2)
    val fooSeries = seriesMap(SortedMap("type" -> "foo"))
    val barSeries = seriesMap(SortedMap("type" -> "bar"))
    fooSeries.iterator.toList should (contain(Point(0, 1)) and (not contain Point(0, 3)))
    barSeries.iterator.toList should (contain(Point(0, 2)) and (not contain Point(0, 3)))
  }

  it should "perform multiple attribute value queries (All filter)" in {
    val storageStub = new PointsStorageStub(shardAttrs = Set("attr"))
    val fooPoint = messagePoint(
      metricName = "metric",
      timestamp = 0,
      value = 1,
      attrs = Seq("type" -> "foo", "attr" -> "foo")
    )
    val barPoint = messagePoint(
      metricName = "metric",
      timestamp = 0,
      value = 2,
      attrs = Seq("type" -> "bar", "attr" -> "foo")
    )
    val junkPoint = messagePoint(
      metricName = "metric",
      timestamp = 0,
      value = 3,
      attrs = Seq("type" -> "foo", "attr" -> "junk")
    )
    writePoints(storageStub, Seq(fooPoint, barPoint, junkPoint))
    val future = storageStub.storage.getPoints(
      "metric",
      (0, 1),
      Map("type" -> AllValueNames, "attr" -> SingleValueName("foo"))
    )
    val seriesMap = toSeriesMap(future)
    seriesMap.size should equal(2)
    val fooSeries = seriesMap(SortedMap("type" -> "foo", "attr" -> "foo"))
    val barSeries = seriesMap(SortedMap("type" -> "bar", "attr" -> "foo"))
    fooSeries.iterator.toList should (contain(Point(0, 1)) and (not contain Point(0, 3)))
    barSeries.iterator.toList should (contain(Point(0, 2)) and (not contain Point(0, 3)))
  }

  it should "perform list value queries" in {
    val storageStub = new PointsStorageStub(shardAttrs = Set("shard"))
    val listPoints = Seq[ListPoint](
      ListPoint(10, Left(Seq(1, 2, 3))),
      ListPoint(11, Left(Seq(4, 5, 6)))
    )
    val excludedPoints = Seq(ListPoint(9, Left(Seq(0))), ListPoint(13, Left(Seq(0))))
    val points = (listPoints ++ excludedPoints).map {
      case ListPoint(timestamp, listValue) =>
        PopeyeTestUtils.createListPoint(
          metric = "metric",
          timestamp = timestamp,
          attributes = Seq("shard" -> "foo"),
          value = listValue
        )
    }
    writePoints(storageStub, points)
    val future = storageStub.storage.getListPoints(
      "metric",
      (10, 12),
      Map("shard" -> SingleValueName("foo"))
    )
    val listPointSeries = toListPoints(future)
    val expectedListPointSeries = ListPointTimeseries(SortedMap("shard" -> "foo"), listPoints)
    listPointSeries should equal(Seq(expectedListPointSeries))
  }

  it should "load downsampled timeseries" in {
    val storageStub = new PointsStorageStub(shardAttrs = Set("a"))
    val point = messagePoint(
      metricName = "metric",
      timestamp = DownsamplingResolution.secondsInDay * 10,
      value = 1,
      attrs = Seq("a" -> "foo")
    )
    def resolveQName(qName: QualifiedName) = {
      val future = storageStub.uniqueId.resolveIdByName(qName, create = true)(5 seconds)
      Await.result(future, Duration.Inf)
    }

    val downsampling = EnabledDownsampling(DownsamplingResolution.Day, AggregationType.Max)
    val SuccessfulConversion(kv) = storageStub.tsdbFormat.convertToKeyValue(
      point,
      qName => Some(resolveQName(qName)),
      10,
      downsampling)
    storageStub.pointsTable.put(new Put(CellUtil.cloneRow(kv)).add(kv))
    val future = storageStub.storage.getPoints(
      "metric",
      (0, DownsamplingResolution.secondsInDay * 11),
      Map("a" -> SingleValueName("foo")),
      downsampling
    )
    val seriesMap = toSeriesMap(future)
    val series = seriesMap(SortedMap("a" -> "foo")).iterator.toList
    series should contain(Point(DownsamplingResolution.secondsInDay * 10, 1))
  }

  def toSeriesMap(groupsIterator: AsyncIterator[PointsSeriesMap]): Map[PointAttributes, PointRope] = {
    val groupsFuture = HBaseStorage.collectSeries(groupsIterator)(executionContext)
    Await.result(groupsFuture, Duration.Inf).seriesMap
  }

  def toListPoints(listPointSeriesIterator: AsyncIterator[Seq[ListPointTimeseries]]): Seq[ListPointTimeseries] = {
    val seriesFuture = AsyncIterator.foldLeft[Seq[ListPointTimeseries], Seq[ListPointTimeseries]](
      listPointSeriesIterator,
      Seq(),
      _ ++ _,
      Promise().future
    )(executionContext)
    Await.result(seriesFuture, 5 seconds)
  }

  def writePoints(state: PointsStorageStub, points: Seq[Message.Point]) {
    Await.result(state.storage.writeMessagePoints(points :_*), 5 seconds)
  }

  def messagePoint(metricName: String, timestamp: Long, value: Long, attrs: Seq[(String, String)]) = {
    PopeyeTestUtils.createPoint(metricName, timestamp, attrs, Left(value))
  }

  def attribute(name: String, value: String) =
    Message.Attribute.newBuilder().setName(name).setValue(value).build()
}
