package popeye.pipeline

import popeye.transport.proto.Message.Point

/**
 * @author Andrey Stepachev
 */
trait PointsSource {

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

trait PointsSourceFactory {
  def newConsumer(topic: String): PointsSource
}
