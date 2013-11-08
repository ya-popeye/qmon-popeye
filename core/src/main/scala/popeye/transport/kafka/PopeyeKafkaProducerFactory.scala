package popeye.transport.kafka

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import popeye.transport.proto.Message.Point
import popeye.transport.proto.{PackedPoints, Message}
import java.io.Closeable

/**
 * @author Andrey Stepachev
 */
trait PopeyeKafkaProducerFactory {
  def newProducer(topic: String): PopeyeKafkaProducer
}

