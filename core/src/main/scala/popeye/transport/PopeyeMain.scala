package popeye.transport

import akka.actor.ActorSystem
import akka.event.LogSource
import scala.concurrent.duration._
import akka.util.Timeout
import com.codahale.metrics.{CsvReporter, JmxReporter, MetricRegistry}
import java.util.concurrent.TimeUnit
import java.io.File
import popeye.ConfigUtil

/**
 * @author Andrey Stepachev
 */
abstract class PopeyeMain(val subsys: String) extends App {
  implicit val timeout: Timeout = 2 seconds
  implicit val system = ActorSystem(subsys, ConfigUtil.loadSubsysConfig(subsys).resolve())
  implicit val metricRegistry = new MetricRegistry()

  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName

    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }
  val log = akka.event.Logging(system, this)
  val config = system.settings.config

  val csvReporter = if (config.getBoolean("metrics.csv.enabled")) {
    val r = CsvReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(new File(config.getString("metrics.csv.directory")))
    r.start(config.getMilliseconds("metrics.csv.period"), MILLISECONDS)
    Some(r)
  } else {
    None
  }

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
      csvReporter foreach {
        _.stop()
      }
    }
  }))
}
