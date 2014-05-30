package popeye.storage.hbase

import popeye.test.PopeyeTestUtils._
import popeye.test.{PopeyeTestUtils, MockitoStubs}
import popeye.pipeline.test.AkkaTestKitSpec
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import popeye.proto.{PackedPoints, Message}
import nl.grons.metrics.scala.Meter
import scala.util.Random
import popeye.storage.hbase.HBaseStorage._
import scala.collection.immutable.SortedMap

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
        Message.Attribute.newBuilder().setName(name).setValue(value).build
    }.asJava
    val point = Message.Point.newBuilder()
      .setMetric(metric)
      .setTimestamp(0)
      .addAllAttributes(attributes)
      .setIntValue(0)
      .build()
    Await.ready(storageStub.storage.writeMessagePoints(point), 5 seconds)
    val points = storageStub.hTable.getScanner(HBaseStorage.PointsFamily).map(_.raw).flatMap {
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
    val points = storageStub.hTable.getScanner(HBaseStorage.PointsFamily).map(_.raw).flatMap {
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
    val timeRanges = (0 to 3).map(id => (id * 3600, (id + 1) * 3600, bytesKey(0, id.toByte)))
    val timeRangeIdMapping = createTimeRangeIdMapping(timeRanges: _*)
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
    import HBaseStorage.ValueNameFilterCondition._
    val future = storageStub.storage.getPoints("my.metric1", (1200, 4801), Map("host" -> SingleValueName("localhost")))
    val groupsMap = toGroupsMap(future)
    val group = groupsMap(SortedMap())
    group.size should equal(1)
    val series = group(SortedMap("host" -> "localhost"))
    series should contain(Point(1200, 1))
    series should (not contain Point(0, 0))
    series should (not contain Point(6000, 5))
  }

  it should "perform multiple attributes queries" in {
    val storageStub = new PointsStorageStub()
    val point = messagePoint(
      metricName = "metric",
      timestamp = 0,
      value = 1,
      attrs = Seq("a" -> "foo", "b" -> "foo")
    )

    writePoints(storageStub, Seq(point))
    import HBaseStorage.ValueNameFilterCondition._
    val future = storageStub.storage.getPoints("metric", (0, 1), Map("a" -> SingleValueName("foo"), "b" -> SingleValueName("foo")))
    val groupsMap = toGroupsMap(future)
    val group = groupsMap(SortedMap())
    group.size should equal(1)
    val series = group(SortedMap("a" -> "foo", "b" -> "foo"))

    series should contain(Point(0, 1))
  }


  it should "perform multiple attribute value queries" in {
    val storageStub = new PointsStorageStub()
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
    import HBaseStorage.ValueNameFilterCondition._
    val future = storageStub.storage.getPoints("metric", (0, 1), Map("type" -> MultipleValueNames(Seq("foo", "bar"))))
    val groupsMap = toGroupsMap(future)
    groupsMap.size should equal(2)
    val fooGroup = groupsMap(SortedMap("type" -> "foo"))
    val barGroup = groupsMap(SortedMap("type" -> "bar"))
    fooGroup.size should equal(1)
    barGroup.size should equal(1)
    fooGroup(SortedMap("type" -> "foo")) should (contain(Point(0, 1)) and (not contain Point(0, 3)))
    barGroup(SortedMap("type" -> "bar")) should (contain(Point(0, 2)) and (not contain Point(0, 3)))
  }

  it should "perform multiple attribute value queries (All filter)" in {
    val storageStub = new PointsStorageStub()
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
      attrs = Seq("type" -> "foo", "attr" -> "bar")
    )
    writePoints(storageStub, Seq(fooPoint, barPoint, junkPoint))
    import HBaseStorage.ValueNameFilterCondition._
    val future = storageStub.storage.getPoints("metric", (0, 1), Map("type" -> AllValueNames, "attr" -> SingleValueName("foo")))
    val groupsMap = toGroupsMap(future)
    groupsMap.size should equal(2)
    val fooGroup = groupsMap(SortedMap("type" -> "foo"))
    val barGroup = groupsMap(SortedMap("type" -> "bar"))
    fooGroup.size should equal(1)
    barGroup.size should equal(1)
    fooGroup(SortedMap("type" -> "foo", "attr" -> "foo")) should (contain(Point(0, 1)) and (not contain Point(0, 3)))
    barGroup(SortedMap("type" -> "bar", "attr" -> "foo")) should (contain(Point(0, 2)) and (not contain Point(0, 3)))
  }

  def toGroupsMap(future: Future[PointsStream]): Map[PointAttributes, PointsGroup] = {
    Await.result(future.flatMap(_.toFuturePointsGroups), 5 seconds).groupsMap
  }

  def writePoints(state: PointsStorageStub, points: Seq[Message.Point]) {
    Await.result(state.storage.writeMessagePoints(points :_*), 5 seconds)
  }

  def messagePoint(metricName: String, timestamp: Long, value: Long, attrs: Seq[(String, String)]) = {
    val builder = Message.Point.newBuilder()
      .setMetric(metricName)
      .setTimestamp(timestamp)
      .setIntValue(value)
    for ((name, value) <- attrs) {
      builder.addAttributes(attribute(name, value))
    }
    builder.build()
  }

  def attribute(name: String, value: String) =
    Message.Attribute.newBuilder().setName(name).setValue(value).build()
}
