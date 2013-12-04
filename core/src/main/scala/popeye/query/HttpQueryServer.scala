package popeye.query

import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import popeye.Logging
import spray.http._
import spray.http.HttpMethods._
import akka.pattern.ask
import java.net.InetSocketAddress
import akka.io.IO
import spray.can.Http
import com.typesafe.config.Config
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import popeye.storage.hbase.HBaseStorage
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition
import popeye.storage.hbase.PointsStream
import popeye.query.HttpQueryServer.PointsStorage
import scala.util.Try
import spray.http.HttpRequest
import spray.http.ChunkedResponseStart
import scala.Some
import spray.http.HttpResponse
import java.io.{PrintWriter, StringWriter}


class HttpQueryServer(storage: PointsStorage, executionContext: ExecutionContext) extends Actor with Logging {
  implicit val eCtx = executionContext
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
        val pointStreamFuture = storage.getPoints(metricName, (startTime.toInt, endTime.toInt), attributes)
        pointStreamFuture.onSuccess {
          case PointsStream(points, None) => savedClient ! HttpResponse(entity = HttpEntity(points.mkString("\n")))
          case PointsStream(points, Some(nextPointsFuture)) =>
            savedClient ! ChunkedResponseStart(HttpResponse(entity = HttpEntity(points.mkString("\n"))))
            val future = nextPointsFuture()
            processNextPoints(savedClient, future)
        }
        pointStreamFuture.onFailure {
          case t: Throwable =>
            val sw = new StringWriter()
            t.printStackTrace(new PrintWriter(sw))
            val errMessage = sw.toString
            savedClient ! ChunkedResponseStart(HttpResponse(entity = HttpEntity(errMessage)))
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

  def parseAttributes(attributesString: String): Either[String, Map[String, ValueNameFilterCondition]] =
    Try {
      import ValueNameFilterCondition._
      val attributes = attributesString.split(";").toList.filter(!_.isEmpty)
      val valueFilters = attributes.map {
        attrPair =>
          val Array(name, valuesString) = attrPair.split("->")
          val valueFilterCondition =
            if (valuesString == "*") {
              All
            } else if (valuesString.contains(" ")) {
              Multiple(valuesString.split(" "))
            } else {
              Single(valuesString)
            }
          (name, valueFilterCondition)
      }
      valueFilters.toMap
    }.toOption.toRight("incorrect attributes format; example: \"?attrs=host->foo;type->bar+foo;port->*\"")

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
    def getPoints(metric: String,
                  timeRange: (Int, Int),
                  attributes: Map[String, ValueNameFilterCondition]): Future[PointsStream]
  }

  def runServer(config: Config, storage: HBaseStorage, system: ActorSystem, executionContext: ExecutionContext) = {
    implicit val timeout: Timeout = 5 seconds
    val pointsStorage = new PointsStorage {
      def getPoints(metric: String,
                    timeRange: (Int, Int),
                    attributes: Map[String, ValueNameFilterCondition]) =
        storage.getPoints(metric, timeRange, attributes)(executionContext)

    }

    val handler = system.actorOf(
      Props.apply(new HttpQueryServer(pointsStorage, executionContext)),
      name = "server-http")

    val hostport = config.getString("server.http.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    IO(Http)(system) ? Http.Bind(
      listener = handler,
      endpoint = addr,
      backlog = config.getInt("server.http.backlog"),
      options = Nil,
      settings = None)
    handler

  }

}
