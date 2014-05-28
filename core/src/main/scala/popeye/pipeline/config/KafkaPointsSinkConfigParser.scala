package popeye.pipeline.config

import popeye.pipeline.kafka.{KafkaPointsSinkConfig, KafkaPointsProducerConfig, KafkaPointsProducer}
import com.typesafe.config.Config

object KafkaPointsSinkConfigParser {
  def parse(config: Config) = {
    val producerConfig = KafkaPointsProducer.producerConfig(config)
    val pointsProducerConfig = new KafkaPointsProducerConfig(config)
    KafkaPointsSinkConfig(producerConfig, pointsProducerConfig)
  }
}
