package popeye.transport.kafka

import popeye.transport.proto.Storage.Ensemble

/**
 * Consume identification. Used to identify batches from kafka pipeline.
 * @param batchId unique id of batch
 * @param offset offset in topic/partition
 * @param partition partition
 */
case class ConsumeId(batchId: Long, offset: Long, partition: Long)

sealed class ConsumeCommand

case class ConsumePending(data: Ensemble, id: ConsumeId) extends ConsumeCommand

case class ConsumeDone(id: ConsumeId) extends ConsumeCommand

case class ConsumeFailed(id: ConsumeId, cause: Throwable) extends ConsumeCommand


