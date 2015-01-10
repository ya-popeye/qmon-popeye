package popeye.query

import java.net.InetSocketAddress

import akka.io.IO
import akka.util.Timeout
import com.typesafe.config.Config
import akka.actor.{Props, Actor, ActorRef, ActorSystem}
import spray.can.Http
import spray.can.server.ServerSettings
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait HttpServerFactory {
  def runServer(config: Config,
                storage: PointsStorage,
                system: ActorSystem,
                executionContext: ExecutionContext): Unit = {
    implicit val timeout: Timeout = 5 seconds

    val hostAndPort = config.getString("http.listen").split(":")
    val httpServer = system.actorOf(Props(new HttpServer(() => createHandler(system, storage, executionContext))))
    IO(Http)(system) ! Http.Bind(
      listener = httpServer,
      endpoint = new InetSocketAddress(hostAndPort(0), hostAndPort(1).toInt),
      backlog = config.getInt("http.backlog"),
      options = Nil,
      settings = serverSettings
    )
  }

  def createHandler(system: ActorSystem, storage: PointsStorage, executionContext: ExecutionContext): ActorRef

  def serverSettings: Option[ServerSettings] = None
}

class HttpServer(createHandler: () => ActorRef) extends Actor {
  override def receive: Receive = {
    case x: Http.Connected => sender ! Http.Register(createHandler())
  }
}