package popeye.query

import akka.actor.{ActorSystem, Props, Actor}
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
import popeye.storage.hbase.HBaseStorage
import scala.concurrent.ExecutionContext.Implicits.global
import spray.http.HttpRequest
import spray.http.HttpResponse


class HttpQueryServer(storage: HBaseStorage) extends Actor with Logging {
  val pointsPathPattern = """/points/([^/]+)""".r

  def receive: Actor.Receive = {
    case x: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      sender ! index

    case request@HttpRequest(GET, Uri.Path(pointsPathPattern(metricName)), _, _, _) =>
      val savedSender = sender
      val query = request.uri.query
      val pointsQuery = for {
        startTime <- query.get("start").toRight("start is not set").right
        endTime <- query.get("end").toRight("end is not set").right
        attributesString <- query.get("attrs").toRight("attributes are not set").right
      } yield {
        val attributes = attributesString.split(";").toList.map {
          attrPair =>
            val Array(name, value) = attrPair.split("->")
            (name, value)
        }
        storage.getPoints(metricName, (startTime.toInt, endTime.toInt), attributes).onSuccess {
          case points: Seq[(Int, String)] => savedSender ! HttpResponse(entity = HttpEntity(points.mkString("\n")))
        }
      }

      for (errorMessage <- pointsQuery.left) {
        savedSender ! HttpResponse(entity = HttpEntity(errorMessage))
      }

    case HttpRequest(GET, Uri.Path(path), _, _, _) =>
      sender ! f"invalid path: $path"

  }

  val index = HttpResponse(
    entity = HttpEntity(`text/html`,
      <html>
        <body>
          <h1>Say hello to
            <i>Popeye</i>
            !</h1>
          <ul>
            <li>
              <a href="/points">Read points</a>
            </li>
            <li>
              <a href="/write">Write points</a>
            </li>
            <li>
              <a href="/q">Query</a>
            </li>
          </ul>
        </body>
      </html>.toString()
    )
  )
}

object HttpQueryServer {

  def runServer(config: Config, storage: HBaseStorage)(implicit system: ActorSystem) = {
    implicit val timeout: Timeout = 5 seconds

    val handler = system.actorOf(
      Props.apply(new HttpQueryServer(storage)),
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
