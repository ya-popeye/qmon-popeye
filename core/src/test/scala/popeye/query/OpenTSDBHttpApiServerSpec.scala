package popeye.query

import org.scalatest.matchers.ShouldMatchers
import OpenTSDBHttpApiServer._
import org.scalatest.EitherValues._
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition._
import popeye.pipeline.test.AkkaTestKitSpec
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import scala.concurrent.{Await, Future}
import popeye.storage.hbase.HBaseStorage.{Point, ValueNameFilterCondition, PointsGroups, PointsStream}
import akka.testkit.TestActorRef
import akka.actor.Props
import spray.http.{HttpResponse, Uri, HttpRequest}
import spray.http.HttpMethods._
import popeye.query.OpenTSDBHttpApiServer.TimeSeriesQuery
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition.SingleValueName
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.immutable.SortedMap

class OpenTSDBHttpApiServerSpec extends AkkaTestKitSpec("http-query") with MockitoSugar with ShouldMatchers {
  val executionContext = system.dispatcher

  behavior of "OpenTSDBHttpApiServer.parseTimeSeriesQuery"

  it should "parse simple query" in {
    val queryString = "avg:metric"
    parseTimeSeriesQuery(queryString).right.value should equal(TimeSeriesQuery("avg", isRate = false, "metric", Map()))
  }

  it should "not parse simple query" in {
    parseTimeSeriesQuery("avg") should be('left)
  }

  it should "ignore 'nointerpolation' flag " in {
    val queryString = "avg:nointerpolation:metric"
    parseTimeSeriesQuery(queryString).right.value should equal(TimeSeriesQuery("avg", isRate = false, "metric", Map()))
  }

  it should "parse rate flag" in {
    val queryString = "avg:rate:metric"
    parseTimeSeriesQuery(queryString).right.value should equal(TimeSeriesQuery("avg", isRate = true, "metric", Map()))
  }

  it should "not confuse metric name with rate flag" in {
    val queryString = "avg:rate"
    parseTimeSeriesQuery(queryString).right.value should equal(TimeSeriesQuery("avg", isRate = false, "rate", Map()))
  }

  it should "parse tags" in {
    val query: Either[String, TimeSeriesQuery] = parseTimeSeriesQuery("avg:metric{single=foo,multiple=foo|bar,all=*}")
    val attrs = Map("single" -> SingleValueName("foo"), "multiple" -> MultipleValueNames(Seq("foo", "bar")), "all" -> AllValueNames)
    query.right.value should equal(TimeSeriesQuery("avg", isRate = false, "metric", attrs))
  }

  it should "return error message on bad tags" in {
    val query: Either[String, TimeSeriesQuery] = parseTimeSeriesQuery("avg:metric{type=foo,hostbar}")
    query.left.value should include("tag")
  }

  behavior of "OpenTSDBHttpApiServer actor"
  implicit val timeout = Timeout(5 seconds)

  it should "render points" in {
    val metricParam = "m=" +
      "max:" +
      "nointerpolation:" +
      "metric{single=foo,multiple=foo|bar,all=*}"
    val timeParams = "start=1970/01/01-00:00:00&end=1970/01/01-00:00:01"

    val uriString = f"/q?$timeParams&$metricParam"

    val fooGroup = Map(
      SortedMap("groupByAttr" -> "foo", "attr" -> "12") -> Seq(Point(0, 1), Point(1, 2)),
      SortedMap("groupByAttr" -> "foo", "attr" -> "00") -> Seq(Point(0, 0), Point(1, 0))
    )
    val barGroup = Map(
      SortedMap("groupByAttr" -> "bar", "attr" -> "34") -> Seq(Point(0, 3), Point(1, 4))
    )

    val groups = Map(
      SortedMap("groupByAttr" -> "foo") -> fooGroup,
      SortedMap("groupByAttr" -> "bar") -> barGroup
    )

    val storage = mock[PointsStorage]
    stub(storage.getPoints(any(), any(), any())).toReturn(Future.successful(PointsStream(PointsGroups(groups))))
    val serverRef = TestActorRef(Props.apply(new OpenTSDBHttpApiServer(storage, executionContext)))
    val future = serverRef ? HttpRequest(GET, Uri(uriString, Uri.ParsingMode.RelaxedWithRawQuery))
    val response = Await.result(future, 5 seconds).asInstanceOf[HttpResponse]
    val responseString = response.entity.asString
    val points = responseString.split("\n")

    points should contain("metric 0 1.0 groupByAttr=foo")
    points should contain("metric 1 2.0 groupByAttr=foo")
    points should contain("metric 0 3.0 groupByAttr=bar")
    points should contain("metric 1 4.0 groupByAttr=bar")

    import ValueNameFilterCondition._
    val attrs = Map(
      "single" -> SingleValueName("foo"),
      "multiple" -> MultipleValueNames(Seq("foo", "bar")),
      "all" -> AllValueNames
    )
    verify(storage).getPoints("metric", (0, 1), attrs)

  }
}
