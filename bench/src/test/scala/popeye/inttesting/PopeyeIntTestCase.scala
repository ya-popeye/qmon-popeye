package popeye.inttesting

import org.scalatest.Matchers
import popeye.Logging
import popeye.clients.{QueryClient, SlicerClient, TsPoint}
import popeye.query.PointsStorage.NameType

class PopeyeIntTestCase(currentTimeSeconds: Int) extends Logging with Matchers {

  val periods = Seq(1000, 2000, 3000)
  val shifts = Seq(500, 1000, 1500)
  val amps = Seq(10, 20, 30)
  val firstTimestamp = currentTimeSeconds - 200000
  val timestamps = firstTimestamp to currentTimeSeconds by 100

  val startTime = firstTimestamp.toInt
  val stopTime = currentTimeSeconds.toInt + 1

  def createTestPoints: Seq[TsPoint] = {
    TestDataUtils.createSinTsPoints("sin", timestamps, periods, amps, shifts, "cluster" -> "popeye")
  }

  def putPoints(slicerHost: String, slicerPort: Int): Unit = {
    info("creating slicer client")
    val slicerClient = SlicerClient.createClient(slicerHost, slicerPort)
    info("slicer client was created")
    try {
      val points: Seq[TsPoint] = createTestPoints
      slicerClient.putPoints(points)
      info("test points was written to socket output stream")
      slicerClient.commit() should be(true)
      info("test points commited")
    } finally {
      slicerClient.close()
    }
  }

  def runTestQueries(queryHost: String, queryPort: Int): Unit = {
    val queryClient = new QueryClient(queryHost, queryPort)
    val points = createTestPoints
    val suggestions = queryClient.getSuggestions("s", NameType.MetricType)
    info(s"suggestions response: $suggestions")
    suggestions should equal(Seq("sin"))
    info(s"startTime: $startTime, stopTime: $stopTime")

    testSingleTimeseries(queryClient, points)
    testAllTimeseries(queryClient, points)
    testGroupByTags(queryClient, points)
  }

  def testGroupByTags(queryClient: QueryClient, points: Seq[TsPoint]) {
    val tags = Map("period" -> "*", "cluster" -> "popeye")
    val query = queryClient.queryJson("sin", "max", tags)
    val results = queryClient.runQuery(startTime, Some(stopTime), Seq(query))
    val expectedPoints = points
      .groupBy(point => point.tags("period"))
      .mapValues {
      points =>
        points.groupBy(_.timestamp)
          .mapValues(points => points.map(_.value.fold(_.toDouble, _.toDouble)).max)
          .toList
          .sortBy { case (timestamp, _) => timestamp}
    }
    for (result <- results) {
      assertEqualTimeseries(result.dps, expectedPoints(result.tags("period")))
    }
  }

  def testAllTimeseries(queryClient: QueryClient, points: Seq[TsPoint]) {
    val tags = Map("cluster" -> "popeye")
    val query = queryClient.queryJson("sin", "max", tags)
    val results = queryClient.runQuery(startTime, Some(stopTime), Seq(query))
    val expectedPoints = points
      .groupBy(_.timestamp)
      .mapValues(points => points.map(_.value.fold(_.toDouble, _.toDouble)).max)
      .toList
      .sortBy { case (timestamp, _) => timestamp}
    results.size should equal(1)
    assertEqualTimeseries(results(0).dps, expectedPoints)
  }

  def testSingleTimeseries(queryClient: QueryClient, points: Seq[TsPoint]): Unit = {
    for {
      period <- periods
      amp <- amps
      shift <- shifts
    } {
      val tags = Map("period" -> period, "amp" -> amp, "shift" -> shift).mapValues(_.toString) + ("cluster" -> "popeye")
      val query = queryClient.queryJson("sin", "max", tags)
      val results = queryClient.runQuery(startTime, Some(stopTime), Seq(query))
      results.size should equal(1)
      val cond = tagsFilter("amp" -> amp.toString, "period" -> period.toString, "shift" -> shift.toString) _
      val expectedPoints = points
        .filter(point => cond(point.tags))
        .map(point => (point.timestamp, point.value.fold(_.toDouble, _.toDouble)))
      assertEqualTimeseries(results(0).dps, expectedPoints)
    }
  }

  private def tagsFilter(filterConds: (String, String)*)(tags: Map[String, String]): Boolean = {
    filterConds.forall {
      case (tagKey, tagValue) => tags(tagKey) == tagValue
    }
  }

  private def assertEqualTimeseries(left: Seq[(Int, Double)], right: Seq[(Int, Double)]) = {
    left.size should equal(right.size)
    for (((leftTime, leftValue), (rightTime, rightValue)) <- left zip right) {
      leftTime should equal(rightTime)
      leftValue shouldBe rightValue +- ((math.abs(rightValue) + math.abs(leftValue)) * 0.00001)
    }
  }


}
