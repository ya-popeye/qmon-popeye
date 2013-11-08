package popeye.transport.kafka

import akka.actor.ActorRef
import scala.concurrent.{Promise, Future, ExecutionContext}
import popeye.transport.proto.{PackedPoints}
import popeye.pipeline.PointsSink

/**
 * @author Andrey Stepachev
 */
class KafkaPointsSink(producer: ActorRef)(implicit eCtx: ExecutionContext) extends PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
    val promise = Promise[Long]()
    KafkaPointsProducer.produce(producer, Some(promise), points)
    val pointsInPack = points.pointsCount
    promise.future map { batchId => pointsInPack.toLong }
  }
}
