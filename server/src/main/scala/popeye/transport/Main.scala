package popeye.transport

import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import akka.io.IO
import spray.can.Http
import popeye.transport.legacy.{TsdbTelnetServer, LegacyHttpHandler}
import popeye.transport.kafka.{KafkaEventConsumer, KafkaEventProducer}
import akka.event.LogSource
import akka.routing.FromConfig
import popeye.uuid.IdGenerator
import popeye.storage.opentsdb.TsdbWriter
import net.opentsdb.core.TSDB
import org.hbase.async.HBaseClient
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.util.Timeout
import java.net.InetSocketAddress
import com.codahale.metrics.{JmxReporter, ConsoleReporter, MetricRegistry}
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.{Gauge => CHGauge}
import java.util.concurrent.TimeUnit

/**
 * @author Andrey Stepachev
 */
object Main extends App {
  implicit val timeout: Timeout = 2 seconds
  implicit val system = ActorSystem("popeye")
  implicit val metricRegistry = new MetricRegistry()


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
  val tsdbActor = system.actorOf(Props(new TsdbWriter(tsdb)).withRouter(FromConfig()), "tsdb-writer")

  val consumer = system.actorOf(KafkaEventConsumer.props(conf, tsdbActor))
  system.registerOnTermination(tsdb.shutdown().joinUninterruptibly())
  system.registerOnTermination(hbc.shutdown().joinUninterruptibly())

  val telnet = system.actorOf(Props(new TsdbTelnetServer(new InetSocketAddress("0.0.0.0", 4444), kafka)))

  val reporter = ConsoleReporter.forRegistry(metricRegistry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build();
  reporter.start(10, TimeUnit.SECONDS);

  val jmxreporter = JmxReporter
    .forRegistry(metricRegistry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .inDomain("popeye.transport")
    .build();
  jmxreporter.start();

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
    def run() {
      system.shutdown()
      jmxreporter.stop()
    }
  }))
}
