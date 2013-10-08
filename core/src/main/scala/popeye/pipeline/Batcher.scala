package popeye.pipeline

import akka.actor.Status.Failure
import akka.actor._
import com.codahale.metrics.{Timer, MetricRegistry}
import com.typesafe.config.Config
import popeye.ConfigUtil._
import popeye.pipeline.BatcherProtocol.FlushPoints
import popeye.transport.proto.Message.Point
import popeye.transport.proto.{PackedPointsBuffer, PointsQueue}
import popeye.{Logging, IdGenerator, Instrumented}
import scala.Some
import scala.annotation.tailrec
import scala.concurrent.Promise
import scala.concurrent.duration._

/**
 * @author Andrey Stepachev
 */
class BatcherMetrics(prefix: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer(s"$prefix.wall-time")
  val sendTimer = metrics.timer(s"$prefix.send-time")
  val pointsMeter = metrics.meter(s"$prefix.points")
  val batchFailedMeter = metrics.meter(s"$prefix.batch-failed")
  val batchCompleteMeter = metrics.meter(s"$prefix.batch-complete")
}

class BatcherConfig(config: Config) {
  val batchWaitTimeout: FiniteDuration = toFiniteDuration(
    config.getMilliseconds("tick"))
  val maxQueued = config.getInt("max-queued")
  val numOfWorkers = config.getInt("workers")
  val highWatermark = config.getInt("high-watermark")
  val lowWatermark = config.getInt("low-watermark")
}

object BatcherWorkerProtocol {

  case class WorkDone(batchId: Long)

  case class CorrelatedPoint(correlationId: Long, sender: ActorRef)(val points: Seq[Point])

  case class ProducePack(batchId: Long, started: Timer.Context)
                        (val buffer: PointsQueue.PartitionBuffer, val promises: Seq[Promise[Long]])

}

object BatcherProtocol {

  sealed class Reply

  case object NeedThrottle$ extends Reply

  case class Done(correlationId: Seq[Long], assignedBatchId: Long) extends Reply

  case class Failed(correlationId: Seq[Long], cause: Throwable) extends Reply

  sealed class Command

  case object FlushPoints extends Command

  case class Pending(batchIdPromise: Option[Promise[Long]] = None)(val data: PackedPointsBuffer) extends Command

}


trait BatcherWorkerActor[Batcher <: BatcherActor] extends Actor with Logging {

  import BatcherWorkerProtocol._

  def batcher: Batcher

  def processBatch(batchId: Long, buffer: PointsQueue.PartitionBuffer)

  override def preStart() {
    super.preStart()
    debug("Starting sender")
    batcher.addWorker(self)
  }

  override def postStop() {
    debug("Stopping sender")
    super.postStop()
  }

  def receive = {
    case p@ProducePack(batchId, started) =>
      val sendctx = batcher.metrics.sendTimer.timerContext()
      try {
        processBatch(batchId, p.buffer)
        debug(s"Sent batch ${p.batchId}")
        batcher.metrics.pointsMeter.mark(p.buffer.points)
        batcher.metrics.batchCompleteMeter.mark()
        withDebug {
          p.promises.foreach {
            promise =>
              debug(s"${self.path} got promise $promise for SUCCESS batch ${p.batchId}")
          }
        }
        p.promises
          .filter(!_.isCompleted) // we should check, that pormise not complete before
          .foreach(_.success(batchId))
      } catch {
        case e: Exception => sender ! Failure(e)
          batcher.metrics.batchFailedMeter.mark()
          withDebug {
            p.promises.foreach {
              promise =>
                log.debug(s"${self.path} got promise for FAILURE $promise for batch ${p.batchId}")
            }
          }
          p.promises
            .filter(!_.isCompleted) // we should check, that pormise not complete before
            .foreach(_.failure(e))
          throw e
      } finally {
        val sended = sendctx.stop.nano
        val elapsed = started.stop.nano
        log.debug(s"batch ${p.batchId} sent in ${sended.toMillis}ms at total ${elapsed.toMillis}ms")
        batcher.addWorker(self)
        sender ! WorkDone(batchId)
      }
  }
}

trait BatcherActor extends Actor with Logging {

  type Config <: BatcherConfig
  type Metrics <: BatcherMetrics

  def config: Config

  def metrics: Metrics

  def idGenerator: IdGenerator

  lazy val pendingPoints = new PointsQueue(config.lowWatermark, config.highWatermark)

  var flusher: Option[Cancellable] = None

  var workQueue = new AtomicList[ActorRef]()

  def addWorker(worker: ActorRef) {
    workQueue.add(worker)
  }

  def spawnWorker(): ActorRef

  override def preStart() {
    super.preStart()
    log.debug(s"Starting ${config.numOfWorkers} workers")

    for (i <- 0 until config.numOfWorkers) {
      spawnWorker()
    }

    import context.dispatcher
    flusher = Some(context.system.scheduler.schedule(config.batchWaitTimeout, config.batchWaitTimeout, self, FlushPoints))
  }

  override def postStop() {
    log.debug("Stopping batcher")
    flusher foreach {
      _.cancel()
    }
    super.postStop()
  }

  def receive: Actor.Receive = {
    case BatcherProtocol.FlushPoints =>
      flushPoints(ignoreMinSize = true)

    case BatcherWorkerProtocol.WorkDone(_) =>
      flushPoints()

    case p@BatcherProtocol.Pending(promise) =>
      pendingPoints.addPending(p.data, p.batchIdPromise)
      flushPoints()
  }

  @tailrec
  private def flushPoints(ignoreMinSize: Boolean = false): Unit = {
    if (workQueue.isEmpty)
      return
    workQueue.headOption() match {
      case Some(worker: ActorRef) =>
        val batchId = idGenerator.nextId()
        val (data, promises) = pendingPoints.consume(ignoreMinSize)
        if (!data.isEmpty) {
          worker ! BatcherWorkerProtocol.ProducePack(batchId, metrics.writeTimer.timerContext())(data.get, promises)
          log.debug(s"Sending ${data.foldLeft(0)({
            (a, b) => a + b.buffer.length
          })} bytes, will trigger ${promises.size} promises")
          flushPoints(ignoreMinSize)
        } else {
          workQueue.add(worker)
        }
      case None =>
        return
    }
  }

}
