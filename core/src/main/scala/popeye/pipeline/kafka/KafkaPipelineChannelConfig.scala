package popeye.pipeline.kafka

import com.typesafe.config.Config
import kafka.producer.ProducerConfig

/**
 * @author Andrey Stepachev
 */
class KafkaPipelineChannelConfig(val producerDispatcher: String,
                                 val consumerDispatcher: String,
                                 val producerConfig: ProducerConfig,
                                 val consumerConfigFactory: (String) => KafkaPointsConsumerConfig,
                                 val popeyeProducerConfig: KafkaPointsProducerConfig,
                                 val topic: String,
                                 val consumerWorkers: Int)

object KafkaPipelineChannelConfig {
  def apply(config: Config): KafkaPipelineChannelConfig = {
    val topic: String = config.getString("topic")
    val producerConfig = config.getConfig("producer").withFallback(config)
    val consumerConfig = config.getConfig("consumer").withFallback(config)
    new KafkaPipelineChannelConfig(
      producerDispatcher = config.getString("producer.dispatcher"),
      consumerDispatcher = config.getString("consumer.dispatcher"),
      producerConfig = KafkaPointsProducer.producerConfig(producerConfig),
      consumerConfigFactory = KafkaPointsConsumer.factory(topic, consumerConfig),
      popeyeProducerConfig = new KafkaPointsProducerConfig(config),
      topic = topic,
      consumerWorkers = config.getInt("consumer.workers")
    )
  }
}
