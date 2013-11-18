package popeye.pipeline.kafka

import java.io.Closeable
import popeye.proto.{PackedPoints, Message}

/**
 * @author Andrey Stepachev
 */
trait PopeyeKafkaProducer extends Closeable {
  def sendPoints(batchId: Long, points: Message.Point*)

  def sendPacked(batchId: Long, buffers: PackedPoints*)
}

trait PopeyeKafkaProducerFactory {
  def newProducer(topic: String): PopeyeKafkaProducer
}
