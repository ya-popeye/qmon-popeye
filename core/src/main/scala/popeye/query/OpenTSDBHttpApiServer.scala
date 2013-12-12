package popeye.query

import akka.actor.{ActorSystem, Props, Actor}
import popeye.Logging
import akka.pattern.ask
import java.net.InetSocketAddress
import akka.io.IO
import spray.can.Http
import com.typesafe.config.Config
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import popeye.storage.hbase.HBaseStorage
import spray.http.{HttpEntity, HttpResponse, HttpRequest}


class OpenTSDBHttpApiServer(storage: PointsStorage, executionContext: ExecutionContext) extends Actor with Logging {
  def receive: Actor.Receive = {
    case x: Http.Connected => sender ! Http.Register(self)
    case request: HttpRequest => sender ! HttpResponse(entity = HttpEntity("not implemented"))
  }
}

object OpenTSDBHttpApiServer extends HttpServerFactory {
  def runServer(config: Config, storage: HBaseStorage, system: ActorSystem, executionContext: ExecutionContext) {
    implicit val timeout: Timeout = 5 seconds
    val pointsStorage = PointsStorage.fromHBaseStorage(storage, executionContext)
    val handler = system.actorOf(
      Props.apply(new OpenTSDBHttpApiServer(pointsStorage, executionContext)),
      name = "server-http")

    val hostport = config.getString("server.http.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    IO(Http)(system) ? Http.Bind(
      listener = handler,
      endpoint = addr,
      backlog = config.getInt("server.http.backlog"),
      options = Nil,
      settings = None)
  }
}

