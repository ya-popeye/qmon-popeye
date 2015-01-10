package popeye.pipeline

import popeye.proto.PackedPoints

/**
 * @author Andrey Stepachev
 */
trait PointsSource {

  type BatchedMessageSet = (Long, PackedPoints)

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