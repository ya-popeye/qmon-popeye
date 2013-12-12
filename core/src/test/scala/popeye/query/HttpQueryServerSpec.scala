package popeye.query

import popeye.pipeline.test.AkkaTestKitSpec
import scala.concurrent.{Await, Future}
import spray.http._
import spray.http.HttpMethods._
import akka.testkit.TestActorRef
import akka.actor.{ActorRef, Actor, Props}
import akka.pattern.ask
import spray.http.HttpRequest
import popeye.storage.hbase.HBaseStorage.{PointsGroups, PointsStream, Point, ValueNameFilterCondition}
import spray.http.HttpResponse
import akka.util.Timeout
import scala.concurrent.duration._
import org.scalatest.matchers.Matcher
import java.nio.charset.Charset
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._

class HttpQueryServerSpec extends AkkaTestKitSpec("http-query") with MockitoSugar {
  implicit val executionContext = system.dispatcher

  implicit val timeout = Timeout(5 seconds)

  behavior of "HttpQueryServer"

  it should "parse query string" in {
    val storage = mock[PointsStorage]
    stub(storage.getPoints(any(), any(), any())).toReturn(Future.successful(PointsStream(PointsGroups(Map.empty))))
    val serverRef = TestActorRef(Props.apply(new HttpQueryServer(storage, executionContext)))
    val attributesParams = "attrs=" +
      "single->foo;" +
      "multiple->foo+bar;" +
      "all->*"
    val aggregationParams = "agrg=max&" +
      "dsagrg=avg&" +
      "dsint=10"

    val uriString = f"/points/metricId?start=0&end=1&$attributesParams&$aggregationParams"

    val future = serverRef ? HttpRequest(GET, Uri(uriString))
    val response = Await.result(future, 5 seconds).asInstanceOf[HttpResponse]
    response.entity.asString must equal("")
    import ValueNameFilterCondition._
    val attrs = Map(
      "single" -> Single("foo"),
      "multiple" -> Multiple(Seq("foo", "bar")),
      "all" -> All
    )
    verify(storage).getPoints("metricId", (0, 1), attrs)
  }

}
