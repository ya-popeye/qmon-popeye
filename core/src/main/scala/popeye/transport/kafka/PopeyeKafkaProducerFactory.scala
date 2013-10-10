package popeye.transport.kafka

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import popeye.transport.proto.Message.Point
import popeye.transport.proto.{PackedPoints, Message}
import java.io.Closeable

trait PopeyeKafkaProducer extends Closeable {
  def sendPoints(batchId: Long, points: Message.Point*)

  def sendPacked(batchId: Long, buffers: PackedPoints*)
}

/**
 * @author Andrey Stepachev
 */
trait PopeyeKafkaProducerFactory {
  def newProducer(topic: String): PopeyeKafkaProducer
}

class PopeyeKafkaProducerFactoryImpl(producerConfig: ProducerConfig)
  extends PopeyeKafkaProducerFactory {

  def newProducer(topic: String): PopeyeKafkaProducer = new PopeyeKafkaProducer {
    val producer = new Producer[Long, Array[Byte]](producerConfig)

    def sendPoints(batchId: Long, points: Point*): Unit = {
      producer.send(new KeyedMessage(topic, batchId, PackedPoints(points).copyOfBuffer))
    }

    def sendPacked(batchId: Long, buffers: PackedPoints*): Unit = {
      producer.send(buffers.map { b =>
        new KeyedMessage(topic, batchId, b.copyOfBuffer)
      }: _*)
    }

    def close() = {
      producer.close()
    }
  }
}

