package popeye.pipeline.kafka

import com.typesafe.config.Config
import kafka.producer.ProducerConfig
import kafka.consumer.ConsumerConfig

/**
 * @author Andrey Stepachev
 */
class KafkaPipelineChannelConfig(val producerDispatcher: String,
                                 val consumerDispatcher: String,
                                 val producerConfig: ProducerConfig,
                                 val pointsConsumerConfig: KafkaPointsConsumerConfig,
                                 val kafkaConsumerConfigFactory: (String) => ConsumerConfig,
                                 val popeyeProducerConfig: KafkaPointsProducerConfig,
                                 val topic: String,
                                 val consumerWorkers: Int)

object KafkaPipelineChannelConfig {
  def apply(config: Config): KafkaPipelineChannelConfig = {
    val topic: String = config.getString("topic")
    val producerConfig = config.getConfig("producer").withFallback(config)
    val consumerConfig = config.getConfig("consumer").withFallback(config)
    val pointsConsumerConfig = KafkaPointsConsumerConfig(consumerConfig)
    new KafkaPipelineChannelConfig(
      producerDispatcher = config.getString("producer.dispatcher"),
      consumerDispatcher = config.getString("consumer.dispatcher"),
      producerConfig = KafkaPointsProducer.producerConfig(producerConfig),
      pointsConsumerConfig = pointsConsumerConfig,
      kafkaConsumerConfigFactory = group => KafkaConsumer.consumerConfig(
        group = group,
        consumerTimeout = pointsConsumerConfig.tick,
        kafkaConfig = consumerConfig
      ),
      popeyeProducerConfig = new KafkaPointsProducerConfig(config),
      topic = topic,
      consumerWorkers = config.getInt("consumer.workers")
    )
  }
}
