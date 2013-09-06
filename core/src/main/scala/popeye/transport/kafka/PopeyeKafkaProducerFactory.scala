package popeye.transport.kafka

import kafka.producer.{ProducerConfig, Producer}
import kafka.consumer.{Consumer, KafkaStream, ConsumerConnector, ConsumerConfig}
import popeye.Logging
import kafka.common.KafkaException

/**
 * @author Andrey Stepachev
 */
trait PopeyeKafkaProducerFactory {
  def newProducer(): Producer[Long, Array[Byte]]
}

class PopeyeKafkaProducerFactoryImpl(producerConfig: ProducerConfig)
  extends PopeyeKafkaProducerFactory {

  def newProducer(): Producer[Long, Array[Byte]] = {
    new Producer[Long, Array[Byte]](producerConfig)
  }
}

