package popeye.pipeline

import akka.actor.Status.Failure
import akka.actor._
import akka.pattern.{AskTimeoutException, ask, after}
import com.codahale.metrics.{Timer, MetricRegistry}
import com.typesafe.config.Config
import popeye.ConfigUtil._
import popeye.pipeline.DispatcherProtocol.{Pending, FlushPoints}
import popeye.pipeline.WorkerProtocol.ProducePack
import popeye.transport.proto.Message.Point
import popeye.{Logging, Instrumented}
import scala.Some
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Await, Future, Promise}
import scala.concurrent.duration._

/**
 * @author Andrey Stepachev
 */
class DispatcherMetrics(prefix: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer(s"$prefix.wall-time")
  val sendTimer = metrics.timer(s"$prefix.send-time")
  val batchFailedMeter = metrics.meter(s"$prefix.batch-failed")
  val batchCompleteMeter = metrics.meter(s"$prefix.batch-complete")
}

class DispatcherConfig(config: Config) {
  val batchWaitTimeout: FiniteDuration = toFiniteDuration(
    config.getMilliseconds("tick"))
  val maxQueued = config.getInt("max-queued")
  val numOfWorkers = config.getInt("workers")
  val highWatermark = config.getInt("high-watermark")
  val lowWatermark = config.getInt("low-watermark")
}

private object WorkerProtocol {

  case class WorkDone(batchId: Long)

  case class CorrelatedPoint(correlationId: Long, sender: ActorRef)(val points: Seq[Point])

  case class ProducePack[Event](batchId: Long, started: Timer.Context)
                               (val batch: Event, val promises: Seq[Promise[Long]])

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
    case p: ProducePack[Batch] =>
      val sendctx = batcher.metrics.sendTimer.timerContext()
      try {
        processBatch(p.batchId, p.batch)
        debug(s"Sent batch ${p.batchId}")
        batcher.metrics.batchCompleteMeter.mark()
        withDebug {
          p.promises.foreach {
            promise =>
              debug(s"${self.path} got promise $promise for SUCCESS batch ${p.batchId}")
          }
        }
        p.promises
          .filter(!_.isCompleted) // we should check, that pormise not complete before
          .foreach(_.success(p.batchId))
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
        val elapsed = p.started.stop.nano
        log.debug(s"batch ${p.batchId} sent in ${sended.toMillis}ms at total ${elapsed.toMillis}ms")
        batcher.addWorker(self)
        sender ! WorkDone(p.batchId)
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

    case p: Pending[Batch] =>
      buffer(p.event, p.batchIdPromise)
      flushBuffered()
  }

  @tailrec
  private def flushBuffered(ignoreMinSize: Boolean = false): Unit = {
    if (workQueue.isEmpty)
      return
    workQueue.headOption() match {
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
      val timeout = after[Long](askTimeout.asInstanceOf[FiniteDuration], using=sch)(
        Future.failed(new AskTimeoutException(s"Query for batch $batchId timed out")))
      Future.firstCompletedOf(Seq(batchPromise.future, timeout))
    } else {
      dispatcherActor ! message
      batchPromise.future
    }
  }
}
