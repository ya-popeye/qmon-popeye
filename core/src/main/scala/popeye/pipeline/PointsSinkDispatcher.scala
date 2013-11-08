package popeye.pipeline

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Deploy, Props, ActorRef, OneForOneStrategy}
import popeye.IdGenerator
import popeye.transport.kafka.{KafkaPointsProducer, KafkaPointsWorker}
import popeye.transport.proto.PackedPoints

class PointsSinkActor(val idGenerator: IdGenerator,
                      val pointsSinkFactory: () => PointsSink,
                      val metrics: DispatcherMetrics)
  extends PointsDispatcherActor {

  type Config = DispatcherConfig
  type Metrics = DispatcherMetrics

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _ ⇒ Restart
  }

  private var idx = 0

  def spawnWorker(): ActorRef = {
    idx += 1
    context.actorOf(
      Props.apply(new KafkaPointsWorker(kafkaClient, this)).withDeploy(Deploy.local),
      "points-sender-" + idx)
  }
}

class PointsSinkWorkerActor(val batcher: PointsSinkActor) extends PointsDispatcherWorkerActor {

  type Batch = Seq[PackedPoints]
  type Batcher = KafkaPointsProducer

  def processBatch(batchId: Long, pack: Seq[PackedPoints]): Unit = {

  }
}
