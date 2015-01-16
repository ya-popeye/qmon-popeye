package popeye.rollup

import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, KeyValueUtil}
import org.scalatest.Matchers
import popeye.proto.Message
import popeye.proto.Message.Point
import popeye.storage.hbase.TsdbFormat.ValueTypes._
import popeye.storage.hbase.{FixedGenerationId, PointsStorageStub, TsdbFormat}
import popeye.test.{AkkaTestKitSpec, PopeyeTestUtils}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class TsdbPointsFilterSpec extends AkkaTestKitSpec("points-storage") with Matchers {

  implicit val exctx = system.dispatcher

  behavior of "TsdbPointsFilter assumptions"

  it should "hold" in {
    import popeye.storage.hbase.TsdbFormat._
    UnsignedBytes.compare(SingleValueTypeStructureId, ListValueTypeStructureId) should be < 0
    uniqueIdGenerationOffset should equal(0)
    val offsetOrder = Seq(metricOffset, valueTypeIdOffset, shardIdOffset, baseTimeOffset, attributesOffset)
    offsetOrder.sorted should equal(offsetOrder)
  }

  behavior of "TsdbPointsFilter.incrementBytes"

  it should "increment bytes" in {
    val array = Array.ofDim[Byte](10)
    Bytes.putBytes(array, 3, Bytes.toBytes(256 * 256), 0, 4)
    TsdbPointsFilter.incrementBytes(array, 3, 4)
    Bytes.toInt(array, 3, 4) should equal(256 * 256 + 1)
  }

  it should "increment bytes (non-trivial case)" in {
    val array = Array.ofDim[Byte](10)
    Bytes.putBytes(array, 3, Bytes.toBytes(255 * 256 + 255), 0, 4)
    TsdbPointsFilter.incrementBytes(array, 3, 4)
    Bytes.toInt(array, 3, 4) should equal(255 * 256 + 255 + 1)
  }

  behavior of "TsdbPointsFilter"
  it should "work as a no-op" in {
    val storageStub: PointsStorageStub = createStorageStub
    val point = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val future = storageStub.storage.writeMessagePoints(point)
    val written = Await.result(future, 5 seconds)
    val scan = new Scan
    scan.setFilter(new TsdbPointsFilter(0, SingleValueTypeStructureId, 0, 3600))
    val results = storageStub.pointsTable.getScanner(scan).asScala.toList
    results.size should equal(1)
  }

  it should "filter a point" in {
    val storageStub: PointsStorageStub = createStorageStub
    val point = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val future = storageStub.storage.writeMessagePoints(point)
    val written = Await.result(future, 5 seconds)
    val scan = new Scan
    scan.setFilter(new TsdbPointsFilter(0, SingleValueTypeStructureId, 3600, 7200))
    val results = storageStub.pointsTable.getScanner(scan).asScala.toList
    results.size should equal(0)
  }

  it should "filter a list point" in {
    val storageStub: PointsStorageStub = createStorageStub
    val point = PopeyeTestUtils.createListPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val future = storageStub.storage.writeMessagePoints(point)
    val written = Await.result(future, 5 seconds)
    val scan = new Scan
    scan.setFilter(new TsdbPointsFilter(0, SingleValueTypeStructureId, 0, 3600))
    val results = storageStub.pointsTable.getScanner(scan).asScala.toList
    results.size should equal(0)
  }

  it should "give a next metric hint" in {
    val currentPoint = PopeyeTestUtils.createPoint(
      metric = "test_1",
      timestamp = 3600,
      attributes = Seq("cluster" -> "test")
    )
    val lastPoint = PopeyeTestUtils.createPoint(
      metric = "test_2",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val points = Seq(
      PopeyeTestUtils.createPoint(
        metric = "test_1",
        timestamp = 3601,
        attributes = Seq("cluster" -> "test")
      ),
      lastPoint
    )
    val pointsFilter = new TsdbPointsFilter(0, SingleValueTypeStructureId, 0, 3600)
    val result = pointsAfterHintRow(currentPoint, points, pointsFilter)
    result should equal(Seq(lastPoint))
  }

  it should "give a timestamp hint" in {
    val currentPoint = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val lastPoint = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 3601,
      attributes = Seq("cluster" -> "test")
    )
    val pointsFilter = new TsdbPointsFilter(0, SingleValueTypeStructureId, 3600, 7200)
    val result = pointsAfterHintRow(currentPoint, Seq(lastPoint), pointsFilter)
    result should equal(Seq(lastPoint))
  }

  it should "give a value id hint" in {
    val currentPoint = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val lastPoint = PopeyeTestUtils.createListPoint(
      metric = "test",
      timestamp = 200,
      attributes = Seq("cluster" -> "test")
    )
    val pointsFilter = new TsdbPointsFilter(0, ListValueTypeStructureId, 0, 3600)
    val result = cellsAfterHintRow(currentPoint, Seq(lastPoint), pointsFilter)
    result.size should equal(1)
  }

  it should "give a next metric hint (too large value id)" in {
    val currentPoint = PopeyeTestUtils.createListPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val lastPoint = PopeyeTestUtils.createListPoint(
      metric = "test_2",
      timestamp = 200,
      attributes = Seq("cluster" -> "test")
    )
    val points = Seq(
      PopeyeTestUtils.createListPoint(
        metric = "test",
        timestamp = 200,
        attributes = Seq("cluster" -> "test")
      ),
      lastPoint
    )
    val pointsFilter = new TsdbPointsFilter(0, SingleValueTypeStructureId, 0, 3600)
    val result = cellsAfterHintRow(currentPoint, points, pointsFilter)
    result.size should equal(1)
  }

  def pointsAfterHintRow(currentPoint: Message.Point,
                         otherPoints: Seq[Message.Point],
                         pointsFilter: TsdbPointsFilter): Seq[Message.Point] = {
    val storageStub: PointsStorageStub = createStorageStub
    val cells = cellsAfterHintRow(currentPoint, otherPoints, pointsFilter, storageStub)
    cells.map(KeyValueUtil.ensureKeyValue).map(storageStub.storage.keyValueToPoint).toList
  }

  def cellsAfterHintRow(currentPoint: Point,
                        otherPoints: Seq[Point],
                        pointsFilter: TsdbPointsFilter,
                        storageStub: PointsStorageStub = createStorageStub): Iterable[Cell] = {
    val currentPointWrite = storageStub.storage.writeMessagePoints(currentPoint)
    Await.result(currentPointWrite, 5 seconds)
    val currentPointSeq = storageStub.pointsTable.getScanner(new Scan()).asScala.flatMap(_.rawCells()).toList
    val Seq(currentKv) = currentPointSeq
    val otherPointsWrite = storageStub.storage.writeMessagePoints(otherPoints: _*)
    Await.result(otherPointsWrite, 5 seconds)
    val hint = pointsFilter.getNextCellHint(currentKv)
    val scan = new Scan(CellUtil.cloneRow(hint))
    storageStub.pointsTable.getScanner(scan).asScala.flatMap(_.listCells().asScala)
  }

  def createStorageStub: PointsStorageStub = {
    val storageStub = new PointsStorageStub(
      shardAttrs = Set("cluster"),
      timeRangeIdMapping = new FixedGenerationId(0))
    storageStub
  }
}
