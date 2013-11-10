package popeye.pipeline

import popeye.proto.{PointsQueue, PackedPoints}
import popeye.IdGenerator
import akka.actor.ActorRef
import scala.concurrent.Promise
import com.typesafe.config.Config
import com.codahale.metrics.MetricRegistry

class PointsDispatcherConfig(config: Config) extends DispatcherConfig(config) {
}

class PointsDispatcherMetrics(prefix: String, override val metricRegistry: MetricRegistry)
  extends DispatcherMetrics(prefix, metricRegistry) {
  val pointsMeter = metrics.meter(s"$prefix.points")
}

trait PointsDispatcherWorkerActor extends WorkerActor {
}

trait PointsDispatcherActor extends DispatcherActor {

  type Batch = Seq[PackedPoints]
  type Config <: PointsDispatcherConfig
  type Metrics <: PointsDispatcherMetrics

  def idGenerator: IdGenerator

  lazy val pendingPoints = new PointsQueue(config.lowWatermark, config.highWatermark)

  def spawnWorker(): ActorRef


  def buffer(buffer: Seq[PackedPoints], batchIdPromise: Option[Promise[Long]]): Unit = {
    buffer.foreach { b =>
      pendingPoints.addPending(b, batchIdPromise)
    }
  }

  def unbuffer(ignoreMinSize: Boolean): Option[WorkerData] = {
    val batchId = idGenerator.nextId()
    val (data, promises) = pendingPoints.consume(ignoreMinSize)
    log.debug(s"Sending ${data.foldLeft(0)({
      (a, b) => a + b.bufferLength
    })} bytes, will trigger ${promises.size} promises")
    if (!data.isEmpty) {
      Some(new WorkerData(batchId, data, promises))
    } else {
      None
    }
  }
}
