package popeye.pipeline

import akka.actor._
import akka.pattern.{AskTimeoutException, after}
import popeye.{Instrumented, Logging}
import popeye.pipeline.DispatcherProtocol.{Pending, FlushPoints}
import popeye.pipeline.WorkerProtocol.ProducePack
import scala.Some
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import com.typesafe.config.Config
import popeye.ConfigUtil._
import popeye.pipeline.WorkerProtocol.ProducePack
import scala.Some
import popeye.pipeline.DispatcherProtocol.Pending
import com.codahale.metrics.{Timer, MetricRegistry}
import scala.util.Failure
import popeye.proto.Message.Point
import java.util.concurrent.atomic.AtomicInteger

class DispatcherConfig(config: Config) {
  val batchWaitTimeout: FiniteDuration = toFiniteDuration(
    config.getMilliseconds("tick"))
  val maxQueued = config.getInt("max-queued")
  val numOfWorkers = config.getInt("workers")
  val highWatermark = config.getInt("high-watermark")
  val lowWatermark = config.getInt("low-watermark")
}

class DispatcherMetrics(prefix: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer(s"$prefix.wall-time")
  val sendTimer = metrics.timer(s"$prefix.send-time")
  val senders = metrics.counter(s"$prefix.senders")
  val sendersTime = metrics.meter(s"$prefix.senders-time")
  val batchTime = metrics.timer(s"$prefix.process-batch-time")
  val batchFailedMeter = metrics.meter(s"$prefix.batch-failed")
  val batchCompleteMeter = metrics.meter(s"$prefix.batch-complete")
  private val id = new AtomicInteger(0)

  def addWorkQueueSizeGauge(queue: AtomicList[ActorRef]) = {
    metrics.gauge(s"$prefix.work-queue-size-${id.getAndIncrement}") {
      queue.size
    }
  }
}

object DispatcherProtocol {

  sealed class Reply

  case object NeedThrottle$ extends Reply

  case class Done(correlationId: Seq[Long], assignedBatchId: Long) extends Reply

  case class Failed(correlationId: Seq[Long], cause: Throwable) extends Reply

  sealed class Command

  case object FlushPoints extends Command

  case class Pending[Event](batchIdPromise: Option[Promise[Long]] = None)(val event: Event)
                           (implicit evidence$1: scala.reflect.ClassTag[Event])
    extends Command

}

private object WorkerProtocol {

  case class WorkDone(batchId: Long)

  case class CorrelatedPoint(correlationId: Long, sender: ActorRef)(val points: Seq[Point])

  case class ProducePack[Event](batchId: Long, started: Timer.Context)
                               (val batch: Event, val promises: Seq[Promise[Long]])

}

trait WorkerActor extends Actor with Logging {

  import WorkerProtocol._

  type Batcher <: DispatcherActor
  type Batch

  def batcher: Batcher

  def processBatch(batchId: Long, pack: Batch)

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
    case p: ProducePack[_] =>
      val sendctx = batcher.metrics.sendTimer.timerContext()
      val batchId = p.batchId
      val promises = p.promises
      try {
        batcher.metrics.senders.inc()
        batcher.metrics.batchTime.time {
          processBatch(p.batchId, p.asInstanceOf[ProducePack[Batch]].batch)
        }
        debug(s"Sent batch ${p.batchId}")
        batcher.metrics.batchCompleteMeter.mark()
        withDebug {
          promises.foreach {
            promise =>
              debug(s"${self.path} got promise $promise for SUCCESS batch ${p.batchId}")
          }
        }
        promises.foreach(_.trySuccess(p.batchId))
      } catch {
        case e: Exception =>
          sender ! Failure(new DispatcherException(s"batch $batchId failed", e))
          batcher.metrics.batchFailedMeter.mark()
          withDebug {
            promises.foreach {
              promise =>
                log.debug(s"${self.path} got promise for FAILURE $promise for batch ${batchId}")
            }
          }
          promises.foreach(_.tryFailure(e))
          throw e
      } finally {
        batcher.metrics.senders.dec()
        val sended = sendctx.stop.nano
        val elapsed = p.started.stop.nano
        batcher.metrics.sendersTime.mark(sended.toMillis)
        log.debug(s"batch ${p.batchId} sent in ${sended.toMillis}ms at total ${elapsed.toMillis}ms")
        batcher.addWorker(self)
        sender ! WorkDone(batchId)
      }
  }
}


trait DispatcherActor extends Actor with Logging {


  type Batch
  type Config <: DispatcherConfig
  type Metrics <: DispatcherMetrics

  class WorkerData(val batchId: Long, val batch: Batch, val batchPromises: Seq[Promise[Long]])

  def config: Config

  def metrics: Metrics

  def spawnWorker(): ActorRef

  def buffer(buffer: Batch, batchIdPromise: Option[Promise[Long]])

  def unbuffer(ignoreMinSize: Boolean): Option[WorkerData]

  var flusher: Option[Cancellable] = None

  var workQueue = new AtomicList[ActorRef]()

  def addWorker(worker: ActorRef) {
    workQueue.add(worker)
  }


  override def preStart() {
    super.preStart()
    log.debug(s"Starting ${config.numOfWorkers} workers")

    for (i <- 0 until config.numOfWorkers) {
      spawnWorker()
    }

    import context.dispatcher
    flusher = Some(context.system.scheduler.schedule(config.batchWaitTimeout, config.batchWaitTimeout, self, FlushPoints))
    metrics.addWorkQueueSizeGauge(workQueue)
  }

  override def postStop() {
    log.debug("Stopping batcher")
    flusher foreach {
      _.cancel()
    }
    super.postStop()
  }

  def receive: Actor.Receive = {
    case DispatcherProtocol.FlushPoints =>
      flushBuffered(ignoreMinSize = true)

    case WorkerProtocol.WorkDone(_) =>
      flushBuffered()

    case p: Pending[_] =>
      buffer(p.asInstanceOf[Pending[Batch]].event, p.batchIdPromise)
      flushBuffered()
  }

  @tailrec
  private def flushBuffered(ignoreMinSize: Boolean = false): Unit = {
    if (workQueue.isEmpty)
      return
    workQueue.headOption match {
      case Some(worker: ActorRef) =>
        unbuffer(ignoreMinSize) match {
          case Some(wd) =>
            worker ! ProducePack[Batch](wd.batchId, metrics.writeTimer.timerContext())(wd.batch, wd.batchPromises)
            flushBuffered(ignoreMinSize)
          case None =>
            workQueue.add(worker)
        }
      case None =>
        return
    }
  }

}

object DispatcherActor {
  def sendBatch[Event](dispatcherActor: ActorRef, batchId: Long, event: Event, askTimeout: Duration)
                      (implicit evidence$1: scala.reflect.ClassTag[Event], sch: Scheduler, eCtx: ExecutionContext)
  : Future[Long] = {
    val batchPromise = Promise[Long]()
    val message = Pending[Event](Some(batchPromise))(event)
    if (askTimeout.isFinite()) {
      dispatcherActor ! message
      val timeout = after[Long](askTimeout.asInstanceOf[FiniteDuration], using = sch)(
        Future.failed(new AskTimeoutException(s"Query for batch $batchId timed out")))
      Future.firstCompletedOf(Seq(batchPromise.future, timeout))
    } else {
      dispatcherActor ! message
      batchPromise.future
    }
  }
}
