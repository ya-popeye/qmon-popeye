package popeye.transport.kafka

import popeye.transport.proto.Message.Event

sealed class ProduceCommand

case object FlushPoints extends ProduceCommand

case class ProducePending(correlationId: Long)(val data: Seq[Event]) extends ProduceCommand

case class ProduceDone(correlationId: Seq[Long], assignedBatchId: Long) extends ProduceCommand

case class ProduceFailed(correlationId: Seq[Long], cause: Throwable) extends ProduceCommand
