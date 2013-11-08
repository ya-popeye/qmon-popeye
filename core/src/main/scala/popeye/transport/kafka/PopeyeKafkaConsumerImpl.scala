package popeye.transport.kafka

import kafka.common.KafkaException
import kafka.consumer.{ConsumerTimeoutException, KafkaStream, ConsumerConnector}
import kafka.message.MessageAndMetadata
import popeye.Logging
import popeye.transport.proto.Message.Point
import popeye.transport.proto.PackedPoints

/**
 * @author Andrey Stepachev
 */
class PopeyeKafkaConsumerImpl(consumerConnector: ConsumerConnector, topic: String) extends PopeyeKafkaConsumer with Logging {

  val stream = topicStream(topic)
  val iterator = stream.iterator()

  def commitOffsets(): Unit = {
    consumerConnector.commitOffsets
  }

  def shutdown(): Unit = {
    iterator.clearCurrentChunk()
    consumerConnector.shutdown()
  }

  private def topicStream(topic: String): KafkaStream[Array[Byte], Array[Byte]] = {
    var streams = Map[String, KafkaStream[Array[Byte], Array[Byte]]]()
    streams.get(topic) match {
      case Some(stream) => stream
      case None =>
        val stream = consumerConnector.createMessageStreams(Map(topic -> 1))
          .getOrElse(topic, throw new KafkaException(s"Unable to get stream for topic $topic")).headOption
        streams = streams.updated(topic, stream.getOrElse(throw new KafkaException(s"Unable to get stream for topic $topic")))
        stream.get
    }
  }

  def hasNext: Boolean = try {
    iterator.hasNext()
  } catch {
    case ex: ConsumerTimeoutException =>
      false
  }

  def consume(): Option[(Long, Seq[Point])] = {
    if (hasNext) {
      try {
        val msg: MessageAndMetadata[Array[Byte], Array[Byte]] = iterator.next()
        Some(PackedPoints.decodeWithBatchId(msg.message))
      } catch {
        case ex: ConsumerTimeoutException =>
          None // ok
      }
    } else {
      None
    }
  }
}
