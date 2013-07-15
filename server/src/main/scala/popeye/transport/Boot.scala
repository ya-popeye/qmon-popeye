package popeye.transport

import akka.actor.{Props, ActorSystem}
import akka.pattern.{ask, pipe}
import akka.io.IO
import spray.can.Http
import popeye.transport.legacy.LegacyHttpHandler
import popeye.transport.kafka.{KafkaEventConsumer, KafkaEventProducer}
import akka.event.LogSource
import akka.routing.FromConfig
import popeye.uuid.IdGenerator
import popeye.storage.opentsdb.TsdbWriterActor
import net.opentsdb.core.TSDB
import org.hbase.async.HBaseClient
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import akka.util.Timeout

/**
 * @author Andrey Stepachev
 */
object Boot extends App {
  implicit val timeout: Timeout = 2 seconds
  implicit val system = ActorSystem("popeye")

  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName

    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }
  val log = akka.event.Logging(system, this)
  implicit val idGenerator = new IdGenerator(1)
  val conf = system.settings.config

  // the handler actor replies to incoming HttpRequests
  val kafka = system.actorOf(KafkaEventProducer.props(conf)
    .withRouter(FromConfig()), "kafka-producer")
  val handler = system.actorOf(Props(new LegacyHttpHandler(kafka)), name = "http")

  import ExecutionContext.Implicits.global
  IO(Http) ? Http.Bind(handler, interface = "0.0.0.0", port = 8080) onSuccess {
    case x =>
  }

  val hbc = new HBaseClient(conf.getString("tsdb.zk.cluster"))
  val tsdb: TSDB = new TSDB(hbc,
    conf.getString("tsdb.table.series"),
    conf.getString("tsdb.table.uids"))
  val tsdbActor = system.actorOf(Props(new TsdbWriterActor(tsdb)).withRouter(FromConfig()), "tsdb-writer")

  val consumer = system.actorOf(KafkaEventConsumer.props(conf, tsdbActor))

  system.registerOnTermination(tsdb.shutdown().joinUninterruptibly())
  system.registerOnTermination(hbc.shutdown().joinUninterruptibly())

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
    def run() {
      system.shutdown()
    }
  }))
}
