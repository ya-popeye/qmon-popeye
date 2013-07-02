package qmon.transport

import akka.actor.{Props, ActorSystem}
import akka.io.IO
import spray.can.Http
import qmon.transport.legacy.LegacyHttpHandler

/**
 * @author Andrey Stepachev
 */
object Boot extends App {
  implicit val system = ActorSystem("qmon")

  // the handler actor replies to incoming HttpRequests
  val handler = system.actorOf(Props[LegacyHttpHandler], name = "legacy-http")

  IO(Http) ! Http.Bind(handler, interface = "0.0.0.0", port = 8080)
}
