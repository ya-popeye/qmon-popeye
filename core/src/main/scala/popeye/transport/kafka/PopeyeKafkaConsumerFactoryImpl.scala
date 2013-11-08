package popeye.transport.kafka

import kafka.consumer.{Consumer, ConsumerConfig}

/**
 * @author Andrey Stepachev
 */
class PopeyeKafkaConsumerFactoryImpl(consumerConfig: ConsumerConfig)
  extends PopeyeKafkaConsumerFactory {
  def newConsumer(topic: String): PopeyeKafkaConsumer = new PopeyeKafkaConsumerImpl(Consumer.create(consumerConfig), topic)
}
