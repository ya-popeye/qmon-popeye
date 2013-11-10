package popeye.transport.server.http

import com.codahale.metrics.MetricRegistry
import popeye.Instrumented

/**
 * @author Andrey Stepachev
 */
class HttpPointsServerMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer("write")
  val readTimer = metrics.timer("read")
  val requestsBatches = metrics.histogram("batches", "size")
  val totalBatches = metrics.meter("batches", "total")
  val failedBatches = metrics.meter("batches", "failed")
}
