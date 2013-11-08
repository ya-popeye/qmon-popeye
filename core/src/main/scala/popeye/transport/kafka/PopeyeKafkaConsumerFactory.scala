package popeye.transport.kafka

trait PopeyeKafkaConsumerFactory {
  def newConsumer(topic: String): PopeyeKafkaConsumer
}
