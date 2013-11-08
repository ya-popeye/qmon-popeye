package popeye.transport.kafka

import com.codahale.metrics.MetricRegistry
import popeye.pipeline.PointsDispatcherMetrics

/**
 * @author Andrey Stepachev
 */
class KafkaProducerMetrics(prefix: String, metricsRegistry: MetricRegistry)
  extends PointsDispatcherMetrics(s"$prefix.producer", metricsRegistry)
