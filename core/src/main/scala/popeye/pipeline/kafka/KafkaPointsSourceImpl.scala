package popeye.pipeline.kafka

import kafka.common.KafkaException
import kafka.consumer._
import popeye.{Instrumented, Logging}
import popeye.proto.Message.Point
import popeye.proto.PackedPoints
import popeye.pipeline.PointsSource
import kafka.message.MessageAndMetadata
import scala.{collection, Some}
import com.codahale.metrics.MetricRegistry

class KafkaPointsSourceImplMetrics(val prefix: String,
                                   val metricRegistry: MetricRegistry) extends Instrumented {
  val consumerTimeouts = metrics.meter(s"$prefix.consume.consumer-timeouts")
}

class KafkaPointsSourceImpl(consumerConnector: ConsumerConnector, topic: String, metrics: KafkaPointsSourceImplMetrics) extends PointsSource with Logging {

  val stream = topicStream(topic)
  val iterator = stream.iterator()
  val deser = new KeyDeserializer()

  def commitOffsets(): Unit = {
    consumerConnector.commitOffsets
  }

  def shutdown(): Unit = {
    iterator.clearCurrentChunk()
    consumerConnector.shutdown()
  }

  private def topicStream(topic: String): KafkaStream[Array[Byte], Array[Byte]] = {
    val streamsMap = consumerConnector.createMessageStreams(Map(topic -> 1))
    val streamOption = streamsMap.get(topic).flatMap(_.headOption)
    streamOption.getOrElse(throw new KafkaException(s"Unable to get stream for topic $topic"))
  }

  def hasNext: Boolean = try {
    iterator.hasNext()
  } catch {
    case ex: ConsumerTimeoutException =>
      metrics.consumerTimeouts.mark()
      false
  }

  def consume(): Option[(Long, Seq[Point])] = {
    if (hasNext) {
      val msg: MessageAndMetadata[Array[Byte], Array[Byte]] = iterator.next()
      val batchId = deser.fromBytes(msg.key)
      Some((batchId, PackedPoints.decodePoints(msg.message)))
    } else {
      None
    }
  }
}
