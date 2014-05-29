package popeye.pipeline.kafka

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import popeye.proto.Message.Point
import popeye.proto.PackedPoints
import popeye.pipeline.{PointsSinkFactory, PointsSink}
import scala.concurrent.Future

class KafkaPointsSink(topic: String, producerConfig: ProducerConfig) extends PointsSink {
  val producer = new Producer[Long, Array[Byte]](producerConfig)

  def sendPoints(batchId: Long, points: Point*): Unit = {
    producer.send(new KeyedMessage(topic, batchId, PackedPoints(points).copyOfBuffer))
    Future.successful(points.length)
  }

  def sendPacked(batchId: Long, buffers: PackedPoints*): Unit = {
    producer.send(buffers.map { b =>
      new KeyedMessage(topic, batchId, b.copyOfBuffer)
    }: _*)
    Future.successful(buffers.foldLeft(0l){(a, b) => a + b.pointsCount})
  }

  def close() = {
    producer.close()
  }

}

class KafkaPointsSinkFactory(producerConfig: ProducerConfig)
  extends PointsSinkFactory {

  def newSender(topic: String): PointsSink = new KafkaPointsSink(topic, producerConfig)
}
