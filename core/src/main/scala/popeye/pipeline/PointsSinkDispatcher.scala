package popeye.pipeline

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Deploy, Props, ActorRef, OneForOneStrategy}
import popeye.IdGenerator
import popeye.proto.PackedPoints
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class PointsSinkDispatcherActor( val config: PointsDispatcherConfig,
                                 val idGenerator: IdGenerator,
                                 val factory: PointsSinkFactory,
                                 val metrics: PointsDispatcherMetrics)
  extends PointsDispatcherActor {

  type Config = PointsDispatcherConfig
  type Metrics = PointsDispatcherMetrics

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _ ⇒ Restart
  }

  private var idx = 0

  def spawnWorker(): ActorRef = {
    idx += 1
    context.actorOf(
      Props.apply(new PointsSinkWorkerActor(factory, this)).withDeploy(Deploy.local),
      "points-sender-" + idx)
  }
}

class PointsSinkWorkerActor(val factory: PointsSinkFactory, val batcher: PointsSinkDispatcherActor) extends PointsDispatcherWorkerActor {

  type Batch = Seq[PackedPoints]
  type Batcher = PointsSinkDispatcherActor

  val sink = factory.newPointsSink()

  override def postStop(): Unit = {
    super.postStop()
  }

  def processBatch(batchId: Long, pack: Seq[PackedPoints]): Unit = {
    pack.foreach { points =>
      Await.result(sink.send(Seq(batchId), points), Duration.Undefined)
    }
  }
}
