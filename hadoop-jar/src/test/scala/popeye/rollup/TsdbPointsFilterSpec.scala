package popeye.rollup

import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, KeyValueUtil}
import org.scalatest.Matchers
import popeye.proto.Message
import popeye.proto.Message.Point
import popeye.storage.QualifiedName
import popeye.storage.hbase.TsdbFormat.ValueTypes._
import popeye.storage.hbase._
import popeye.test.{AkkaTestKitSpec, PopeyeTestUtils}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import TsdbFormat._

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
    val storageStub: PointsStorageStub = createStorageStub()
    val point = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val future = storageStub.storage.writeMessagePoints(point)
    val written = Await.result(future, 5 seconds)
    val scan = new Scan
    scan.setFilter(createPointsFilter(baseTimeStartSeconds = 0, baseTimeStopSeconds = MAX_TIMESPAN))
    val results = storageStub.pointsTable.getScanner(scan).asScala.toList
    results.size should equal(1)
  }

  it should "filter a point by timestamp" in {
    val storageStub: PointsStorageStub = createStorageStub()
    val point = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val future = storageStub.storage.writeMessagePoints(point)
    val written = Await.result(future, 5 seconds)
    val scan = new Scan
    scan.setFilter(createPointsFilter(baseTimeStartSeconds = MAX_TIMESPAN, baseTimeStopSeconds = MAX_TIMESPAN * 2))
    val results = storageStub.pointsTable.getScanner(scan).asScala.toList
    results.size should equal(0)
  }

  it should "filter a point by generation id" in {
    val storageStub: PointsStorageStub = createStorageStub()
    val point = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val future = storageStub.storage.writeMessagePoints(point)
    val written = Await.result(future, 5 seconds)
    val scan = new Scan
    scan.setFilter(createPointsFilter(generationId = 1, baseTimeStartSeconds = 0, baseTimeStopSeconds = MAX_TIMESPAN))
    val results = storageStub.pointsTable.getScanner(scan).asScala.toList
    results.size should equal(0)
  }

  it should "filter a list point" in {
    val storageStub: PointsStorageStub = createStorageStub()
    val point = PopeyeTestUtils.createListPoint(
      metric = "test",
      timestamp = 100,
      attributes = Seq("cluster" -> "test")
    )
    val future = storageStub.storage.writeMessagePoints(point)
    val written = Await.result(future, 5 seconds)
    val scan = new Scan
    scan.setFilter(createPointsFilter(baseTimeStartSeconds = 0, baseTimeStopSeconds = MAX_TIMESPAN))
    val results = storageStub.pointsTable.getScanner(scan).asScala.toList
    results.size should equal(0)
  }

  it should "give a next metric hint" in {
    val currentPoint = PopeyeTestUtils.createPoint(
      metric = "test_1",
      timestamp = MAX_TIMESPAN,
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
        timestamp = MAX_TIMESPAN + 1,
        attributes = Seq("cluster" -> "test")
      ),
      lastPoint
    )
    val pointsFilter = createPointsFilter(baseTimeStartSeconds = 0, baseTimeStopSeconds = MAX_TIMESPAN)
    val result = pointsAfterHintRow(currentPoint, points, pointsFilter)
    result should equal(Seq(lastPoint))
  }

  it should "give a timestamp hint" in {
    val currentPoint = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 1,
      attributes = Seq("cluster" -> "test")
    )
    val lastPoint = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = MAX_TIMESPAN + 1,
      attributes = Seq("cluster" -> "test")
    )
    val pointsFilter = createPointsFilter(baseTimeStartSeconds = MAX_TIMESPAN, baseTimeStopSeconds = MAX_TIMESPAN * 2)
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
    val pointsFilter = createPointsFilter(
      valueTypeId = ListValueTypeStructureId,
      baseTimeStartSeconds = 0,
      baseTimeStopSeconds = MAX_TIMESPAN
    )
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
    val pointsFilter = createPointsFilter(baseTimeStartSeconds = 0, baseTimeStopSeconds = MAX_TIMESPAN)
    val result = cellsAfterHintRow(currentPoint, points, pointsFilter)
    result.size should equal(1)
  }

  it should "give a generation hint" in {
    val generationIdMapping = PopeyeTestUtils.createGenerationIdMapping(
      (0, MAX_TIMESPAN, 0),
      (MAX_TIMESPAN, MAX_TIMESPAN * 2, 1)
    )
    val storageStub = createStorageStub(generationIdMapping)
    val currentPoint = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 0,
      attributes = Seq("cluster" -> "test")
    )
    val lastPoint = PopeyeTestUtils.createPoint(
      metric = "test_2",
      timestamp = 0,
      attributes = Seq("cluster" -> "test")
    )
    val points = Seq(
      PopeyeTestUtils.createPoint(
        metric = "test",
        timestamp = MAX_TIMESPAN,
        attributes = Seq("cluster" -> "test")
      ),
      lastPoint
    )
    val pointsFilter = createPointsFilter(
      generationId = 1,
      baseTimeStartSeconds = MAX_TIMESPAN,
      baseTimeStopSeconds = MAX_TIMESPAN * 2
    )
    val result = cellsAfterHintRow(currentPoint, points, pointsFilter, storageStub)
    result.size should equal(1)
  }

  it should "filter by downsampling resolution" in {
    val storageStub = createStorageStub()
    val point = PopeyeTestUtils.createPoint(
      metric = "test",
      timestamp = 0,
      attributes = Seq("cluster" -> "test")
    )
    val resolution = DownsamplingResolution.Hour
    val downsampling = EnabledDownsampling(resolution, AggregationType.Max)
    def resolveId(qname: QualifiedName) =
      Await.result(storageStub.uniqueId.resolveIdByName(qname, create = true)(5 seconds), Duration.Inf)
    val SuccessfulConversion(downsampledKv) = storageStub.tsdbFormat.convertToKeyValue(
      point = point,
      idCache = resolveId _ andThen Some.apply,
      currentTimeSeconds = 0,
      downsampling = downsampling
    )
    val SuccessfulConversion(kv) = storageStub.tsdbFormat.convertToKeyValue(
      point = point,
      idCache = resolveId _ andThen Some.apply,
      currentTimeSeconds = 0,
      downsampling = NoDownsampling
    )
    storageStub.pointsTable.put(new Put(CellUtil.cloneRow(kv)).add(kv))
    storageStub.pointsTable.put(new Put(CellUtil.cloneRow(downsampledKv)).add(downsampledKv))
    val scan = new Scan
    scan.setFilter(
      createPointsFilter(
        downsamplingResolutionId = DownsamplingResolution.getId(resolution),
        baseTimeStartSeconds = 0,
        baseTimeStopSeconds = MAX_TIMESPAN
      )
    )
    val results = storageStub.pointsTable.getScanner(scan).asScala.toList
    results.size should equal(1)
    results.head.rawCells().length should equal(1)
    results.head.rawCells().head should equal(downsampledKv)
  }



  def createPointsFilter(generationId: Short = 0,
                         downsamplingResolutionId: Int = 0,
                         valueTypeId: Byte = SingleValueTypeStructureId,
                         baseTimeStartSeconds: Int = 0,
                         baseTimeStopSeconds: Int = 0) = {
    new TsdbPointsFilter(
      generationId,
      downsamplingResolutionId,
      valueTypeId,
      baseTimeStartSeconds,
      baseTimeStopSeconds
    )
  }

  def pointsAfterHintRow(currentPoint: Message.Point,
                         otherPoints: Seq[Message.Point],
                         pointsFilter: TsdbPointsFilter,
                         generationIdMapping: GenerationIdMapping = new FixedGenerationId(0)): Seq[Message.Point] = {
    val storageStub = createStorageStub(generationIdMapping)
    val cells = cellsAfterHintRow(currentPoint, otherPoints, pointsFilter, storageStub)
    cells.map(KeyValueUtil.ensureKeyValue).map(storageStub.storage.keyValueToPoint).toList
  }

  def cellsAfterHintRow(currentPoint: Point,
                        otherPoints: Seq[Point],
                        pointsFilter: TsdbPointsFilter,
                        storageStub: PointsStorageStub = createStorageStub()): Iterable[Cell] = {
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

  def createStorageStub(generationIdMapping: GenerationIdMapping = new FixedGenerationId(0)): PointsStorageStub = {
    val storageStub = new PointsStorageStub(
      shardAttrs = Set("cluster"),
      timeRangeIdMapping = generationIdMapping)
    storageStub
  }
}
