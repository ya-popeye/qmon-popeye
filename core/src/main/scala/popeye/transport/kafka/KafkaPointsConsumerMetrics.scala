package popeye.transport.kafka

import com.codahale.metrics.MetricRegistry
import popeye.Instrumented

/**
 * @author Andrey Stepachev
 */
class KafkaPointsConsumerMetrics(val prefix: String,
                                 val metricRegistry: MetricRegistry) extends Instrumented {
  val consumeTimer = metrics.timer(s"$prefix.consume.time")
  val decodeFailures = metrics.meter(s"$prefix.consume.decode-failures")
}
