package popeye.storage.hbase

import HBasePointConsumerProtocol._
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.routing.FromConfig
import com.codahale.metrics.MetricRegistry
import com.google.protobuf.InvalidProtocolBufferException
import com.typesafe.config.Config
import java.util.Properties
import kafka.consumer._
import kafka.message.MessageAndMetadata
import popeye.ConfigUtil._
import popeye.transport.proto.Message.Point
import popeye.transport.proto.{PackedPoints, Message}
import popeye.{Instrumented, Logging}
import scala.Option
import scala.Some
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.{Success, Failure}
import popeye.transport.kafka.{PopeyeKafkaConsumerFactoryImpl, PopeyeKafkaConsumerFactory}

/**
 * @author Andrey Stepachev
 */

class ConsumerInitializationException extends Exception

class BatchProcessingFailedException extends Exception

case class HBasePointConsumerMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val consumeTimer = metrics.timer("hbase.time")
  val pointsMeter = metrics.meter("hbase.points")
  val batchSizeHist = metrics.histogram("hbase.batch.size")
  val batchCompleteHist = metrics.meter("hbase.batch.complete")
  val batchFailedHist = metrics.meter("hbase.batch.complete")
  val batchDecodeFailuresMeter = metrics.meter("hbase.bached.decode-errors")
  val writeTimer = metrics.timer("hbase.write.write")
  val writeBatchSizeHist = metrics.histogram("hbase.write.batch-size.write")
  val incomingBatchSizeHist = metrics.histogram("hbase.write.batch-size.incoming")
}

object HBasePointConsumerProtocol {

  sealed class ConsumeCommand

  case object CheckAvailable

  case class ConsumeDone(batches: Traversable[Long]) extends ConsumeCommand

  case class ConsumeFailed(batches: Traversable[Long], cause: Throwable) extends ConsumeCommand

}

class HBasePointConsumer(config: Config, storage: PointsStorage, factory: PopeyeKafkaConsumerFactory,
                         val metrics: HBasePointConsumerMetrics)
  extends Actor with Logging {

  import ExecutionContext.Implicits.global
  import HBasePointConsumer._

  val topic = config.getString("kafka.points.topic")

  val consumer = factory.newConsumer()

  lazy val maxBatchSize = config.getLong("hbase.kafka.consumer.batch-size")
  lazy val checkTick = toFiniteDuration(config.getMilliseconds("hbase.kafka.consumer.check-tick"))
  lazy val writeTimeout = toFiniteDuration(config.getMilliseconds("hbase.kafka.consumer.write-timeout"))

  var checker: Option[Cancellable] = None

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _ ⇒ Restart
    }

  override def preStart() {
    super.preStart()
    // jitter to prevent rebalance deadlock
    //context.system.scheduler.scheduleOnce(Random.nextInt(10) seconds, self, ConsumeDone(Nil))
    debug(s"Starting HBasePointConsumer topic $topic")
    checker = Some(context.system.scheduler.schedule(checkTick, checkTick, self, CheckAvailable))
  }

  override def postStop() {
    checker foreach {
      _.cancel()
    }
    super.postStop()
    debug(s"Stoping HBasePointConsumer topic $topic")
  }

  def receive = {
    case CheckAvailable =>
      doNext()

    case ConsumeDone(batches) =>
      consumer.commitOffsets()
      metrics.batchCompleteHist.mark()
      doNext()

    case ConsumeFailed(batches, ex) =>
      error("Batches ${batches.size} failed", ex)
      metrics.batchFailedHist.mark()
      throw new BatchProcessingFailedException
  }

  def doNext() = {
    val batchIds = new ArrayBuffer[Long]
    val batch = new ArrayBuffer[Message.Point]
    val tctx = metrics.consumeTimer.timerContext()

    val iterator = consumer.iterateTopic(topic)
    while (iterator.hasNext && batch.size < maxBatchSize) {
      try {
        iterator.next() match {
          case Some((batchId, points)) =>
            metrics.batchSizeHist.update(points.size)
            batchIds += batchId
            batch ++= points
          case None =>
            // timeout, we should break on hasNext == false
        }
      } catch {
        case e: InvalidProtocolBufferException =>
          metrics.batchDecodeFailuresMeter.mark()
      }
    }
    if (batchIds.size > 0) {
      metrics.pointsMeter.mark(batch.size)
      tctx.close()
      withDebug {
        batch.filter(_.getMetric.startsWith("test")).foreach(p => debug(s"Point: ${p.toString}"))
      }
      sendBatch(batchIds, batch)
    }
  }


  def sendBatch(batches: Seq[Long], events: Seq[Point]): Int = {
    val ctx = metrics.writeTimer.timerContext()
    metrics.writeBatchSizeHist.update(events.size)
    val future: Future[Int] = storage.writePoints(events)
    future.onComplete {
      case Success(x) =>
        val nanos = ctx.stop()
        self ! ConsumeDone(batches)
        debug(s"${batches.size} batches sent in ${NANOSECONDS.toMillis(nanos)}ms")
      case Failure(cause: Throwable) =>
        val nanos = ctx.stop()
        self ! ConsumeFailed(batches, cause)
        error(s"${batches.size} batches failed to send (after ${NANOSECONDS.toMillis(nanos)}ms)", cause)

    }
    Await.result(future, writeTimeout)
  }

}

object HBasePointConsumer extends Logging {

  def props(config: Config, storage: PointsStorage, factory: PopeyeKafkaConsumerFactory)(implicit system: ActorSystem, metricRegistry: MetricRegistry) = {
    Props.apply(new HBasePointConsumer(config, storage, factory, HBasePointConsumerMetrics(metricRegistry)))
  }

  def start(config: Config, storage: PointsStorage)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    val factory = new PopeyeKafkaConsumerFactoryImpl(consumerConfig(config))
    system.actorOf(props(config, storage, factory)
      .withRouter(FromConfig())
      .withDispatcher("hbase.dispatcher"), "hbase-writer")
  }

  def consumerConfig(globalConfig: Config): ConsumerConfig = {
    val config: Config = globalConfig.getConfig("hbase.kafka.consumer.config")
    val consumerProps: Properties = config
    val timeout = globalConfig.getMilliseconds("hbase.kafka.consumer.timeout")
    consumerProps.put("consumer.timeout.ms", timeout.toString)
    consumerProps.put("group.id", globalConfig.getString("hbase.kafka.consumer.group"))
    new ConsumerConfig(consumerProps)
  }
}




