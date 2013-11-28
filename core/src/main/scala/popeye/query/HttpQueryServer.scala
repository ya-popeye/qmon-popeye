package popeye.query

import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import popeye.Logging
import spray.http._
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import akka.pattern.ask
import java.net.InetSocketAddress
import akka.io.IO
import spray.can.Http
import com.typesafe.config.Config
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import popeye.storage.hbase.HBaseStorage
import scala.concurrent.ExecutionContext.Implicits.global
import spray.http.HttpRequest
import popeye.storage.hbase.PointsStream
import spray.http.ChunkedResponseStart
import scala.Some
import spray.http.HttpResponse
import popeye.query.HttpQueryServer.PointsStorage
import scala.util.Try


class HttpQueryServer(storage: PointsStorage) extends Actor with Logging {
  val pointsPathPattern = """/points/([^/]+)""".r

  case class SendNextPoints(client: ActorRef, points: PointsStream)

  def receive: Actor.Receive = {
    case x: Http.Connected => sender ! Http.Register(self)

    case request@HttpRequest(GET, Uri.Path(pointsPathPattern(metricName)), _, _, _) =>
      val savedClient = sender
      val query = request.uri.query
      val pointsQuery = for {
        startTime <- query.get("start").toRight("start is not set").right
        endTime <- query.get("end").toRight("end is not set").right
        attributesString <- query.get("attrs").toRight("attributes are not set").right
        attributes <- parseAttributes(attributesString).right
      } yield {
        storage.getPoints(metricName, (startTime.toInt, endTime.toInt), attributes).onSuccess {
          case PointsStream(points, None) => savedClient ! HttpResponse(entity = HttpEntity(points.mkString("\n")))
          case PointsStream(points, Some(nextPointsFuture)) =>
            savedClient ! ChunkedResponseStart(HttpResponse(entity = HttpEntity(points.mkString("\n"))))
            val future = nextPointsFuture()
            processNextPoints(savedClient, future)
        }
      }

      for (errorMessage <- pointsQuery.left) {
        savedClient ! HttpResponse(entity = HttpEntity(errorMessage))
      }

    case HttpRequest(GET, Uri.Path(path), _, _, _) =>
      sender ! f"invalid path: $path"

    case SendNextPoints(client: ActorRef, pointsStream: PointsStream) =>
      client ! MessageChunk(pointsStream.points.mkString("\n", "\n", ""))
      if (pointsStream.next.isDefined) {
        val future = pointsStream.next.get()
        processNextPoints(client, future)
      } else {
        client ! ChunkedMessageEnd()
      }

  }

  def parseAttributes(attributes: String): Either[String, List[(String, String)]] = Try {
    attributes.split(";").toList.filter(!_.isEmpty).map {
      attrPair =>
        val Array(name, value) = attrPair.split("->")
        (name, value)
    }
  }.toOption.toRight("incorrect attributes format; example: \"?attrs=host->foo;host->bar\"")

  def processNextPoints(savedClient: ActorRef, future: Future[PointsStream]) = {
    future.onSuccess {case pointsStream: PointsStream => self ! SendNextPoints(savedClient, pointsStream)}
    future.onFailure {
      case e: Exception =>
        savedClient ! ChunkedMessageEnd()
        info(e)
    }
  }

}

object HttpQueryServer {

  trait PointsStorage {
    def getPoints(metric: String, timeRange: (Int, Int), attributes: List[(String, String)]): Future[PointsStream]
  }

  def runServer(config: Config, storage: HBaseStorage)(implicit system: ActorSystem) = {
    implicit val timeout: Timeout = 5 seconds
    val pointsStorage = new PointsStorage {
      def getPoints(metric: String, timeRange: (Int, Int), attributes: List[(String, String)]) =
        storage.getPoints(metric, timeRange, attributes)
    }

    val handler = system.actorOf(
      Props.apply(new HttpQueryServer(pointsStorage)),
      name = "server-http")

    val hostport = config.getString("server.http.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    IO(Http) ? Http.Bind(
      listener = handler,
      endpoint = addr,
      backlog = config.getInt("server.http.backlog"),
      options = Nil,
      settings = None)
    handler

  }

}
