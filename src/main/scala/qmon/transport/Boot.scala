package qmon.transport

import akka.actor.{Props, ActorSystem}
import akka.io.IO
import spray.can.Http

/**
 * @author Andrey Stepachev
 */
object Boot extends App {
  implicit val system = ActorSystem("qmon")

  // the handler actor replies to incoming HttpRequests
  val handler = system.actorOf(Props[LegacyHttpHandler], name = "legacy-http")

  IO(Http) ! Http.Bind(handler, interface = "localhost", port = 8080)
}
