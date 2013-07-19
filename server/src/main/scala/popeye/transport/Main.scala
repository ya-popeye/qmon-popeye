package popeye.transport

import akka.actor.ActorSystem
import popeye.transport.legacy.{TsdbTelnetServer, LegacyHttpHandler}
import popeye.transport.kafka.{KafkaEventConsumer, KafkaEventProducer}
import akka.event.LogSource
import popeye.uuid.IdGenerator
import popeye.storage.opentsdb.TsdbWriter
import scala.concurrent.duration._
import akka.util.Timeout
import com.codahale.metrics.{JmxReporter, ConsoleReporter}
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
  implicit val idGenerator = new IdGenerator(
    config.getLong("generator.worker.id"),
    config.getLong("generator.datacenter.id")
  )
  val config = system.settings.config

  val kafkaProducer = KafkaEventProducer.start(config, idGenerator)

  LegacyHttpHandler.bind(config, kafkaProducer)
  TsdbTelnetServer.start(config, kafkaProducer)

  val tsdbSink = TsdbWriter.start(config)
  val consumer = system.actorOf(KafkaEventConsumer.props(config, tsdbSink))

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
