package popeye.transport.kafka

import kafka.producer.{ProducerConfig, Producer}

/**
 * @author Andrey Stepachev
 */
trait PopeyeKafkaClient {
  def newProducer(): Producer[Long, Array[Byte]]
}

class PopeyeKafkaClientImpl(producerConfig: ProducerConfig) extends PopeyeKafkaClient {
  def newProducer(): Producer[Long, Array[Byte]] = {
    new Producer[Long, Array[Byte]](producerConfig)
  }
}
