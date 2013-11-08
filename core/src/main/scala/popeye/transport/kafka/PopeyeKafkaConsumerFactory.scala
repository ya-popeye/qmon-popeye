package popeye.transport.kafka

import kafka.common.KafkaException
import kafka.consumer._
import kafka.message.MessageAndMetadata
import popeye.Logging
import popeye.transport.proto.Message.Point
import popeye.transport.proto.{Message, PackedPoints}
import scala.collection.mutable.ArrayBuffer

/**
 * @author Andrey Stepachev
 */
trait PopeyeKafkaConsumer {

  type BatchedMessageSet = (Long, Seq[Point])

  /**
   * Iterate messages in topic stream
   * @throws InvalidProtocolBufferException in case of bad message
   * @return Some((batchId, messages)) or None in case of read timeout
   */
  def consume(): Option[BatchedMessageSet]

  /**
   * Commit offsets consumed so far
   */
  def commitOffsets(): Unit

  /** Shutdown this consumer */
  def shutdown(): Unit
}

trait PopeyeKafkaConsumerFactory {
  def newConsumer(topic: String): PopeyeKafkaConsumer
}

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

class PopeyeKafkaConsumerFactoryImpl(consumerConfig: ConsumerConfig)
  extends PopeyeKafkaConsumerFactory {
  def newConsumer(topic: String): PopeyeKafkaConsumer = new PopeyeKafkaConsumerImpl(Consumer.create(consumerConfig), topic)
}
