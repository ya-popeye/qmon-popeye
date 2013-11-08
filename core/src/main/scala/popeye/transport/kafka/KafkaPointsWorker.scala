package popeye.transport.kafka

import popeye.pipeline.PointsDispatcherWorkerActor
import popeye.transport.proto.PackedPoints
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy.Restart

/**
 * @author Andrey Stepachev
 */
class KafkaPointsWorker(kafkaClient: PopeyeKafkaProducerFactory,
                        val batcher: KafkaPointsProducer)
  extends PointsDispatcherWorkerActor {

  type Batch = Seq[PackedPoints]
  type Batcher = KafkaPointsProducer

  val producer = kafkaClient.newProducer(batcher.config.topic)

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _ => Restart
  }

  override def postStop() {
    super.postStop()
    producer.close()
  }

  def processBatch(batchId: Long, buffer: Seq[PackedPoints]): Unit = {
    producer.sendPacked(batchId, buffer :_*)
  }
}
