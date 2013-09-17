package popeye.transport.kafka

import kafka.common.KafkaException
import kafka.consumer._
import kafka.message.MessageAndMetadata
import popeye.Logging
import popeye.transport.proto.Message.Point
import popeye.transport.proto.{Message, PackedPoints}

/**
 * @author Andrey Stepachev
 */
trait PopeyeKafkaConsumer {
  def iterateTopic(topic: String): Iterator[Option[(Long, Seq[Message.Point])]]

  def commitOffsets(): Unit
  def shutdown(): Unit
}

trait PopeyeKafkaConsumerFactory {
  def newConsumer(): PopeyeKafkaConsumer
}

class PopeyeKafkaConsumerImpl(consumerConnector: ConsumerConnector) extends PopeyeKafkaConsumer with Logging {

  private[this] var streams = Map[String, KafkaStream[Array[Byte], Array[Byte]]]()

  def commitOffsets(): Unit = {
    consumerConnector.commitOffsets
  }

  def shutdown(): Unit = {
    consumerConnector.shutdown()
  }

  private def topicStream(topic: String): KafkaStream[Array[Byte], Array[Byte]] = {
    streams.get(topic) match {
      case Some(stream) => stream
      case None =>
        val stream = consumerConnector.createMessageStreams(Map(topic -> 1))
          .getOrElse(topic, throw new KafkaException(s"Unable to get stream for topic $topic")).headOption
        streams = streams.updated(topic, stream.getOrElse(throw new KafkaException(s"Unable to get stream for topic $topic")))
        stream.get
    }
  }

  /**
   * Iterate messages in topic stream
   * @throws InvalidProtocolBufferException in case of bad message
   * @param topic
   * @return Some((batchId, message)) or None in case of read timeout
   */
  def iterateTopic(topic: String): Iterator[Option[(Long, Seq[Message.Point])]] = {
    val stream = topicStream(topic)
    new Iterator[Option[(Long, Seq[Message.Point])]] {
      val iterator = stream.iterator()

      def hasNext: Boolean = try {
        iterator.hasNext()
      } catch {
        case ex: ConsumerTimeoutException =>
          false
      }

      def next(): Option[(Long, Seq[Point])] = {
        try {
          val msg: MessageAndMetadata[Array[Byte], Array[Byte]] = iterator.next()
          Some(PackedPoints.decodeWithBatchId(msg.message))
        } catch {
          case ex: ConsumerTimeoutException =>
            None // ok
        }
      }
    }
  }
}

class PopeyeKafkaConsumerFactoryImpl(consumerConfig: ConsumerConfig)
  extends PopeyeKafkaConsumerFactory {
  def newConsumer(): PopeyeKafkaConsumer = new PopeyeKafkaConsumerImpl(Consumer.create(consumerConfig))
}
