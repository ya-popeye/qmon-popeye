package popeye

import akka.actor.ActorSystem
import akka.event.LogSource
import akka.util.Timeout
import com.codahale.metrics.{CsvReporter, JmxReporter, MetricRegistry}
import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import com.typesafe.config.Config

trait MetricsConfiguration {

  def initMetrics(systemConfig: Config): MetricRegistry = {
    val metricRegistry = new MetricRegistry()

    val csvReporter = if (systemConfig.getBoolean("metrics.csv.enabled")) {
      val r = CsvReporter
        .forRegistry(metricRegistry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build(new File(systemConfig.getString("metrics.csv.directory")))
      r.start(systemConfig.getMilliseconds("metrics.csv.period"), MILLISECONDS)
      Some(r)
    } else {
      None
    }

    val jmxreporter = JmxReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .inDomain("popeye.transport")
      .build()
    jmxreporter.start()

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      def run() {
        jmxreporter.stop()
        csvReporter foreach {
          _.stop()
        }
      }
    }))
    metricRegistry
  }

}
