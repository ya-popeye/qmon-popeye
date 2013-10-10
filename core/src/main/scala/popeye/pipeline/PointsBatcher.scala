package popeye.pipeline

import akka.actor.ActorRef
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.IdGenerator
import popeye.transport.proto.{PackedPoints, PointsQueue}
import scala.concurrent.Promise

/**
 * @author Andrey Stepachev
 */
class PointsDispatcherMetrics(prefix: String, override val metricRegistry: MetricRegistry)
  extends DispatcherMetrics(prefix, metricRegistry) {
  val pointsMeter = metrics.meter(s"$prefix.points")
}

class PointsDispatcherConfig(config: Config) extends DispatcherConfig(config) {
}

trait PointsBatcherWorkerActor extends WorkerActor {
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
