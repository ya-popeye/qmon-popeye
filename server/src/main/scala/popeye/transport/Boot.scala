package popeye.transport

import akka.actor.{Props, ActorSystem}
import akka.io.IO
import spray.can.Http
import popeye.transport.legacy.LegacyHttpHandler
import popeye.transport.kafka.EventProducer
import akka.event.LogSource
import akka.routing.FromConfig
import popeye.uuid.IdGenerator
import popeye.storage.opentsdb.TsdbWriterActor
import net.opentsdb.core.TSDB
import org.hbase.async.HBaseClient

/**
 * @author Andrey Stepachev
 */
object Boot extends App {
  implicit val system = ActorSystem("popeye")
  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName

    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }
  val log = akka.event.Logging(system, this)
  implicit val idGenerator = new IdGenerator(1)
  val conf = system.settings.config
  val hbc = new HBaseClient(conf.getString("tsdb.zk.cluster"))
  val tsdb: TSDB = new TSDB(hbc,
    conf.getString("tsdb.table.series"),
    conf.getString("tsdb.table.uids"))

  // the handler actor replies to incoming HttpRequests
  val kafka = system.actorOf(EventProducer.props(conf)
    .withRouter(FromConfig()), "kafka-producer")
  val handler = system.actorOf(Props(new LegacyHttpHandler(kafka)), name = "http")
  IO(Http) ! Http.Bind(handler, interface = "0.0.0.0", port = 8080)
  system.actorOf(Props(new TsdbWriterActor(tsdb)))
  system.registerOnTermination(tsdb.shutdown().joinUninterruptibly())
  system.registerOnTermination(hbc.shutdown().joinUninterruptibly())
}
