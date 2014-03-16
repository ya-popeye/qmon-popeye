package popeye.pipeline

import popeye.proto.{PointsQueue, PackedPoints}
import popeye.IdGenerator
import akka.actor.ActorRef
import scala.concurrent.Promise
import com.typesafe.config.Config
import com.codahale.metrics.MetricRegistry
import java.util.concurrent.atomic.AtomicInteger

class PointsDispatcherConfig(config: Config) extends DispatcherConfig(config) {
}

class PointsDispatcherMetrics(prefix: String, override val metricRegistry: MetricRegistry)
  extends DispatcherMetrics(prefix, metricRegistry) {
  val pointsMeter = metrics.meter(s"$prefix.points")
  val batchSize = metrics.histogram(s"$prefix.points-batch-size")
  val queueSizeAfterFlush = metrics.histogram(s"$prefix.queue-size-after-flush")
  val queueSizeAfterWorkDone = metrics.histogram(s"$prefix.queue-size-after-work-done")
  val queueEmpty = metrics.meter(s"$prefix.queue-empty")
  private val id = new AtomicInteger(0)

  def addQueueGauge(queue: PointsQueue) = {
    metrics.gauge(s"$prefix.queue-size-${id.getAndIncrement}") {
      queue.stat.points
    }
  }
}

trait PointsDispatcherWorkerActor extends WorkerActor {
  type Batch = Seq[PackedPoints]
}

trait PointsDispatcherActor extends DispatcherActor {

  type Batch = Seq[PackedPoints]
  type Config <: PointsDispatcherConfig
  type Metrics <: PointsDispatcherMetrics

  def idGenerator: IdGenerator

  lazy val pendingPoints = {
    val queue = new PointsQueue(config.lowWatermark, config.highWatermark)
    metrics.addQueueGauge(queue)
    queue
  }

  def spawnWorker(): ActorRef


  def buffer(buffer: Seq[PackedPoints], batchIdPromise: Option[Promise[Long]]): Unit = {
    buffer.foreach { b =>
      pendingPoints.addPending(b, batchIdPromise)
    }
  }

  def unbuffer(ignoreMinSize: Boolean): Option[WorkerData] = {
    val batchId = idGenerator.nextId()
    val (data, promises) = pendingPoints.consume(ignoreMinSize)
    if (pendingPoints.stat.points == 0) {
      metrics.queueEmpty.mark()
    }
    if (!data.isEmpty) {
      metrics.batchSize.update(data.map(_.pointsCount).sum)
      log.debug(s"Sending ${data.foldLeft(0)({
        (a, b) => a + b.bufferLength
      })} bytes, will trigger ${promises.size} promises")
      Some(new WorkerData(batchId, data, promises))
    } else {
      None
    }
  }
}
