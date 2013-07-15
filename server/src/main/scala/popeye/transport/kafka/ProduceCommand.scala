package popeye.transport.kafka

import popeye.transport.proto.Message.Batch

sealed class ProduceCommand

case class ProducePending(data: Batch, correlationId: Long) extends ProduceCommand

case class ProduceDone(correlationId: Long, assignedBatchId: Long) extends ProduceCommand

case class ProduceFailed(correlationId: Long, cause: Throwable) extends ProduceCommand
