package popeye.transport.kafka

import popeye.transport.proto.Message.Batch

sealed class ProduceCommand

case class ProducePending(correlationId: Long)(val data: Batch) extends ProduceCommand

case class ProduceDone(correlationId: Long, assignedBatchId: Long) extends ProduceCommand

case class ProduceFailed(correlationId: Long, cause: Throwable) extends ProduceCommand
