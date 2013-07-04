package qmon.transport

import akka.actor.{Props, ActorSystem}
import akka.io.IO
import spray.can.Http
import qmon.transport.legacy.LegacyHttpHandler
import qmon.transport.kafka.EventProducer
import akka.event.LogSource
import akka.routing.FromConfig
import qmon.transport.ConfigUtil._

/**
 * @author Andrey Stepachev
 */
object Boot extends App {
  implicit val system = ActorSystem("qmon")
  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName

    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }
  val log = akka.event.Logging(system, this)

  // the handler actor replies to incoming HttpRequests
  val kafka = {
    val props: Props = EventProducer.fromConfig(system.settings.config)
    system.actorOf(props.withRouter(FromConfig()), "kafka-producer")
  }
  val handler = system.actorOf(Props(new LegacyHttpHandler(kafka)), name = "legacy-http")
  IO(Http) ! Http.Bind(handler, interface = "0.0.0.0", port = 8080)

}
