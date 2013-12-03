package popeye.query

import popeye.pipeline.test.AkkaTestKitSpec
import popeye.query.HttpQueryServer.PointsStorage
import scala.concurrent.{Await, Future}
import spray.http._
import spray.http.HttpMethods._
import akka.testkit.TestActorRef
import akka.actor.{ActorRef, Actor, Props}
import akka.pattern.ask
import spray.http.HttpRequest
import popeye.storage.hbase.{Point, PointsStream}
import spray.http.HttpResponse
import akka.util.Timeout
import scala.concurrent.duration._
import org.scalatest.matchers.Matcher
import java.nio.charset.Charset
import popeye.storage.hbase.PointsLoaderUtils.ValueNameFilterCondition
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._

class HttpQueryServerSpec extends AkkaTestKitSpec("http-query") with MockitoSugar {
  implicit val executionContext = system.dispatcher

  implicit val timeout = Timeout(5 seconds)

  behavior of "HttpQueryServer"

  it should "return points in one chunk" in {
    val points = Seq(Point(0, 111), Point(1, 222))
    val storage = createPointsStorage(Seq(points))
    val serverRef = TestActorRef(Props.apply(new HttpQueryServer(storage, executionContext)))
    val uri = createUri("dummy", (0, 0), List())
    val future = (serverRef ? HttpRequest(GET, uri)).map {
      case response: HttpResponse =>
        response.entity.asString must (include("111") and include("222"))

      case x => fail(f"wrong response type: $x")
    }
    Await.result(future, 5 seconds)
  }

  it should "return points in multiple chunks" in {
    val chunks = Seq(Point(0, 111), Point(1, 222), Point(2, 333)).grouped(1).toSeq
    val chunkMatchers = Seq(include("111"), include("222"), include("333")).toIndexedSeq

    val storage = createPointsStorage(chunks)
    val serverRef = TestActorRef(Props.apply(new HttpQueryServer(storage, executionContext)))
    val uri = createUri("dummy", (0, 0), List())
    val requestMessage = HttpRequest(GET, uri)
    val clientRef = TestActorRef(Props.apply(new ChunkedResponseTester(chunkMatchers, serverRef, requestMessage)))

    val future = clientRef ? "start test"
    Await.result(future, 5 seconds)
  }

  it should "parse query string" in {
    val storage = mock[PointsStorage]
    stub(storage.getPoints(any(), any(), any())).toReturn(Future.successful(PointsStream(Nil)))
    val serverRef = TestActorRef(Props.apply(new HttpQueryServer(storage, executionContext)))
    val attributesString = "attrs=" +
      "single->foo;" +
      "multiple->foo+bar;" +
      "all->*"
    val uriString = "/points/metricId?start=0&end=1&" + attributesString

    val future = serverRef ? HttpRequest(GET, Uri(uriString))
    Await.result(future, 5 seconds)
    import popeye.storage.hbase.PointsLoaderUtils.ValueNameFilterCondition._
    val attrs = Map(
      "single" -> Single("foo"),
      "multiple" -> Multiple(Seq("foo", "bar")),
      "all" -> All
    )
    verify(storage).getPoints("metricId", (0, 1), attrs)
  }

  class ChunkedResponseTester(chunkMatchers: IndexedSeq[Matcher[String]], server: TestActorRef[_], testMessage: Any) extends Actor {
    var chunkNumber = 0
    var testStarter: ActorRef = null

    def receive: Actor.Receive = {
      case "start test" =>
        testStarter = sender
        server ! testMessage

      case ChunkedResponseStart(response: HttpResponse) =>
        val responseString = response.entity.asString
        checkChunk(responseString)

      case message: MessageChunk =>
        checkChunk(new String(message.data.toByteArray, Charset.forName("UTF-8")))

      case _: ChunkedMessageEnd =>
        chunkNumber must equal(chunkMatchers.size)
        testStarter ! "test completed"

      case x => fail(f"wrong response type: $x")
    }

    def checkChunk(chunk: String) {
      try {
        chunk must chunkMatchers(chunkNumber)
      } finally {
        chunkNumber += 1
      }
    }
  }

  type Points = Seq[Point]

  def createPointsStorage(chunks: Seq[Points]) = new PointsStorage {
    def getPoints(metric: String,
                  timeRange: (Int, Int),
                  attributes: Map[String, ValueNameFilterCondition]): Future[PointsStream] =
      Future.successful(toPointsStream(chunks))

  }

  def toPointsStream(chunks: Seq[Points]) = {
    val lastStream = PointsStream(chunks.last)
    chunks.init.foldRight(lastStream) {
      case (points, nextStream) =>
        PointsStream(points, Future.successful(nextStream))
    }
  }

  def createUri(metricName: String, timeRange: (Int, Int), attributes: List[(String, String)]) = {
    val (start, end) = timeRange
    val attrsString = attributes.map {case (name, value) => f"$name->$value"}.mkString(";")
    Uri(f"/points/$metricName?start=$start&end=$end&attrs=$attrsString")
  }
}
