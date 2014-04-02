package popeye.query

import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.collection.immutable.SortedMap
import popeye.storage.hbase.{PointsStreamTestUtils, HBaseStorage}
import scala.concurrent.duration._
import java.util.concurrent.Executors
import popeye.pipeline.test.AkkaTestKitSpec
import akka.testkit.TestActorRef
import akka.actor.Props
import akka.pattern.ask
import spray.http.{StatusCodes, HttpResponse, Uri, HttpRequest}
import spray.http.HttpMethods._
import popeye.storage.hbase.HBaseStorage.PointsGroups
import org.mockito.Matchers._
import akka.util.Timeout
import org.scalatest.matchers.Matcher

class HealthCheckServerSpec extends AkkaTestKitSpec("http-query") with MockitoSugar {
  implicit val timeout = Timeout(5 seconds)

  behavior of "HealthCheckTool"

  it should "report good health" in {
    testHealth(
      numberOfDistinctTagValuesInFirstTimeInterval = 10,
      numberOfDistinctTagValuesInSecondTimeInterval = 11
    ) should be(true)
  }

  it should "report bad health" in {
    testHealth(
      numberOfDistinctTagValuesInFirstTimeInterval = 15,
      numberOfDistinctTagValuesInSecondTimeInterval = 10
    ) should be(false)
  }

  behavior of "HealthCheckTool.getAllDistinctAttributeValues"

  it should "get all distinct values" in {
    implicit val ectx = newExecutionContext
    val points = pointsStream("test", 10, 3)
    val distinctValues = HealthCheckTool.getAllDistinctAttributeValues(points)
    Await.result(distinctValues, 5 seconds) should equal((1 to 10).map(_.toString).toSet)
  }

  def testHealth(numberOfDistinctTagValuesInFirstTimeInterval: Int,
                 numberOfDistinctTagValuesInSecondTimeInterval: Int) = {
    import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition._
    implicit val ectx = newExecutionContext
    val metricName = "test"
    val fixedAttrs = Seq("name" -> "value")
    val countAttr = "host"
    val valueFilters = Map("name" -> Single("value"), countAttr -> All)
    val storage = mock[PointsStorage]
    val firstStream = pointsStream(countAttr, numberOfDistinctTagValuesInFirstTimeInterval, 3)
    stub(storage.getPoints(metricName, (70, 80), valueFilters)).toReturn(Future.successful(firstStream))
    val secondStream = pointsStream(countAttr, numberOfDistinctTagValuesInSecondTimeInterval, 3)
    stub(storage.getPoints(metricName, (80, 90), valueFilters)).toReturn(Future.successful(secondStream))
    val future = HealthCheckTool.checkHealth(
      storage,
      metricName,
      fixedAttrs,
      countAttr,
      checkTime = 100,
      timeInterval = 10
    )
    Await.result(future, 5 seconds)
  }

  def pointsStream(countAttrName: String, numberOfDistinctValues: Int, sizeOfChunk: Int = 3) = {
    val valueChunks = (1 to numberOfDistinctValues).map(_.toString).grouped(sizeOfChunk).toList
    val groups = valueChunks.map {
      values =>
        val groupKeyAttrs = values.map(v => Seq(countAttrName -> v))
        createPointsGroup(groupKeyAttrs)
    }
    PointsStreamTestUtils.createStream(groups)
  }

  def createPointsGroup(groupKeyAttributes: Seq[Seq[(String, String)]]) = {
    val groupsKeys = groupKeyAttributes.map(attrs => SortedMap(attrs: _*))
    val emptyGroup: HBaseStorage.NamedPointsGroup = Map.empty
    PointsGroups(groupsKeys.map(key => (key, emptyGroup)).toMap)
  }

  behavior of "HealthCheckServer"

  it should "report good health" in {
    val executionContext = newExecutionContext
    val storage = mock[PointsStorage]
    val metricName = "test"
    val fixedAttrs = "foo+bar,name+value"
    val countAttr = "host"
    val timeInterval = 10
    val points = pointsStream(countAttr, 10, 3)
    stub(storage.getPoints(any(), any(), any())).toReturn(Future.successful(points))
    val serverRef = TestActorRef(Props.apply(new HealthCheckServer(storage, executionContext)))
    val uriString = f"/?metric=$metricName&fixed_attrs=$fixedAttrs&count_attr=$countAttr&time_interval=$timeInterval"
    val future = (serverRef ? HttpRequest(GET, Uri(uriString))).mapTo[HttpResponse]
    Await.result(future, 5 seconds).entity.asString should equal("{ isHealthy : \"true\" }")
  }

  it should "send internal server error status code" in {
    val executionContext = newExecutionContext
    val storage = mock[PointsStorage]
    val metricName = "test"
    val fixedAttrs = "foo+bar"
    val countAttr = "host"
    val timeInterval = 10
    stub(storage.getPoints(any(), any(), any())).toReturn(Future.failed(new RuntimeException()))
    val serverRef = TestActorRef(Props.apply(new HealthCheckServer(storage, executionContext)))
    val uriString = f"/?metric=$metricName&fixed_attrs=$fixedAttrs&count_attr=$countAttr&time_interval=$timeInterval"
    val future = (serverRef ? HttpRequest(GET, Uri(uriString))).mapTo[HttpResponse]
    Await.result(future, 5 seconds).status should equal(StatusCodes.InternalServerError)
  }

  it should "handle empty fixed-attrs requests" in {
    val executionContext = newExecutionContext
    val storage = mock[PointsStorage]
    val metricName = "test"
    val fixedAttrs = ""
    val countAttr = "host"
    val timeInterval = 10
    val points = pointsStream(countAttr, 10, 3)
    stub(storage.getPoints(any(), any(), any())).toReturn(Future.successful(points))
    val serverRef = TestActorRef(Props.apply(new HealthCheckServer(storage, executionContext)))
    val uriString = f"/?metric=$metricName&fixed_attrs=$fixedAttrs&count_attr=$countAttr&time_interval=$timeInterval"
    val future = (serverRef ? HttpRequest(GET, Uri(uriString))).mapTo[HttpResponse]
    Await.result(future, 5 seconds).entity.asString should equal("{ isHealthy : \"true\" }")
  }

  it should "handle no fixed-attrs requests" in {
    val executionContext = newExecutionContext
    val storage = mock[PointsStorage]
    val metricName = "test"
    val countAttr = "host"
    val timeInterval = 10
    val points = pointsStream(countAttr, 10, 3)
    stub(storage.getPoints(any(), any(), any())).toReturn(Future.successful(points))
    val serverRef = TestActorRef(Props.apply(new HealthCheckServer(storage, executionContext)))
    val uriString = f"/?metric=$metricName&count_attr=$countAttr&time_interval=$timeInterval"
    val future = (serverRef ? HttpRequest(GET, Uri(uriString))).mapTo[HttpResponse]
    Await.result(future, 5 seconds).entity.asString should equal("{ isHealthy : \"true\" }")
  }

  it should "send \'bad request\' responses" in {
    val metricName = "test"
    val fixedAttrs = "foo+bar"
    val countAttr = "host"
    val timeInterval = 10
    testBadRequestResponse(
      f"/?fixed_attrs=$fixedAttrs&count_attr=$countAttr&time_interval=$timeInterval",
      include("metric")
    )
    testBadRequestResponse(
      f"/?metric=$metricName&fixed_attrs=$fixedAttrs&time_interval=$timeInterval",
      include("count attribute")
    )
    testBadRequestResponse(
      f"/?metric=$metricName&fixed_attrs=$fixedAttrs&count_attr=$countAttr",
      include("time interval")
    )
  }


  def testBadRequestResponse(uriString: String, errorMessageMatcher: Matcher[String]) {
    val executionContext = newExecutionContext
    val storage = mock[PointsStorage]
    stub(storage.getPoints(any(), any(), any())).toReturn(Future.failed(new RuntimeException()))
    val serverRef = TestActorRef(Props.apply(new HealthCheckServer(storage, executionContext)))
    val future = (serverRef ? HttpRequest(GET, Uri(uriString))).mapTo[HttpResponse]
    val response = Await.result(future, 5 seconds)
    response.status should equal(StatusCodes.BadRequest)
    response.entity.asString should errorMessageMatcher
  }

  def newExecutionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)
}
