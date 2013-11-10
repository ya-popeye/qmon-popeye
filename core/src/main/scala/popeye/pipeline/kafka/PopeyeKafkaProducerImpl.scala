package popeye.pipeline.kafka

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import popeye.proto.Message.Point
import popeye.proto.PackedPoints

class PopeyeKafkaProducerImpl(topic: String, producerConfig: ProducerConfig) extends PopeyeKafkaProducer {
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

class PopeyeKafkaProducerFactoryImpl(producerConfig: ProducerConfig)
  extends PopeyeKafkaProducerFactory {

  def newProducer(topic: String): PopeyeKafkaProducer = new PopeyeKafkaProducerImpl(topic, producerConfig)
}
