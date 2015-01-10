package popeye

import com.codahale.metrics.jvm.{ThreadStatesGaugeSet, MemoryUsageGaugeSet, GarbageCollectorMetricSet}
import com.codahale.metrics.{CsvReporter, JmxReporter, MetricRegistry}
import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import com.typesafe.config.Config

trait MetricsConfiguration {

  def initMetrics(systemConfig: Config): MetricRegistry = {
    val metricRegistry = new MetricRegistry()
    metricRegistry.registerAll(new GarbageCollectorMetricSet())
    metricRegistry.registerAll(new MemoryUsageGaugeSet())
    metricRegistry.registerAll(new ThreadStatesGaugeSet())

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
