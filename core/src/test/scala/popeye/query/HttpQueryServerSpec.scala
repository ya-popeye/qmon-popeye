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
import popeye.storage.hbase.PointsStream
import spray.http.HttpResponse
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.matchers.Matcher
import java.nio.charset.Charset

class HttpQueryServerSpec extends AkkaTestKitSpec("http-query") {

  type Points = Seq[(Int, String)]

  implicit val timeout = Timeout(5 seconds)

  behavior of "HttpQueryServer"

  it should "return points in one chunk" in {
    val points = Seq((123, "321"), (0, "0"))
    val storage = createPointsStorage(Seq(points))
    val serverRef = TestActorRef(Props.apply(new HttpQueryServer(storage)))
    val uri = createUri("dummy", (0, 0), List())
    val future = (serverRef ? HttpRequest(GET, uri)).map {
      case response: HttpResponse =>
        val responseString = response.entity.asString
        println(responseString)
        responseString must include("321")
        responseString must include("0")

      case x => fail(f"wrong response type: $x")
    }
    Await.result(future, 5 seconds)
  }

  it should "return points in multiple chunks" in {
    val chunks = Seq((123, "first"), (0, "middle"), (2, "last")).grouped(1).toSeq
    val chunkMatchers = Seq(include("first"), include("middle"), include("last")).toIndexedSeq

    val storage = createPointsStorage(chunks)
    val serverRef = TestActorRef(Props.apply(new HttpQueryServer(storage)))
    val uri = createUri("dummy", (0, 0), List())
    val requestMessage = HttpRequest(GET, uri)
    val clientRef = TestActorRef(Props.apply(new ChunkedResponseTester(chunkMatchers, serverRef, requestMessage)))

    val future = clientRef ? "start test"
    Await.result(future, 5 seconds)
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

  def createPointsStorage(chunks: Seq[Points]) = new PointsStorage {
    def getPoints(metric: String, timeRange: (Int, Int), attributes: List[(String, String)]) =
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
