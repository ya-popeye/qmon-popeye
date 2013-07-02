package qmon.transport

import akka.actor.{Props, ActorSystem}
import akka.io.IO
import spray.can.Http
import qmon.transport.legacy.LegacyHttpHandler
import qmon.transport.kafka.EventsProducerActor
import com.typesafe.config.{ConfigValue, Config}
import java.util.Properties
import scala.collection.JavaConversions.asScalaSet
import akka.event.LogSource
import java.util.Map.Entry
import akka.routing.FromConfig

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
  val kafka = system.actorOf(Props(new EventsProducerActor("events",
    system.settings.config.getConfig("kafka.producer")))
    .withRouter(FromConfig()), "kafka-producer")
  val handler = system.actorOf(Props(new LegacyHttpHandler(kafka)), name = "legacy-http")
  IO(Http) ! Http.Bind(handler, interface = "0.0.0.0", port = 8080)

  implicit def toProperties(config: Config): Properties = {
    val p = new Properties()
    config.entrySet().foreach({
      e: Entry[String, ConfigValue] => {
        System.out.println(e.toString)
        System.out.flush()
        p.setProperty(e.getKey, e.getValue.unwrapped().toString())
      }
    })
    p
  }
}
