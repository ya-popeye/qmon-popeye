package popeye.transport.kafka

import com.typesafe.config.Config

/**
 * @author Andrey Stepachev
 */
class KafkaPointsConsumerConfig(config: Config) {
  val topic = config.getString("topic")
  val group = config.getString("group")
  val batchSize = config.getInt("batch-size")
  val maxLag = config.getMilliseconds("max-lag")
}
