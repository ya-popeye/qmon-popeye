package popeye.transport.kafka

import popeye.transport.proto.PackedPointsBuffer
import scala.concurrent.Promise

sealed class ProduceReply

case object ProduceNeedThrottle extends ProduceReply

case class ProduceDone(correlationId: Seq[Long], assignedBatchId: Long) extends ProduceReply

case class ProduceFailed(correlationId: Seq[Long], cause: Throwable) extends ProduceReply

sealed class ProduceCommand

case object FlushPoints extends ProduceCommand

case class ProducePending(batchIdPromise: Option[Promise[Long]] = None)(val data: PackedPointsBuffer) extends ProduceCommand


