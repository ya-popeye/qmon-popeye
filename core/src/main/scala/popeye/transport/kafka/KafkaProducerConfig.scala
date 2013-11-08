package popeye.transport.kafka

import com.typesafe.config.Config
import popeye.pipeline.PointsDispatcherConfig

/**
 * @author Andrey Stepachev
 */
class KafkaProducerConfig(config: Config)
  extends PointsDispatcherConfig(config.getConfig("producer")) {

  val topic = config.getString("topic")
}
