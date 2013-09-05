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

class HBasePointConsumer(config: Config, storage: PointsStorage, val metrics: HBasePointConsumerMetrics)
  extends Actor with Logging {

  import ExecutionContext.Implicits.global
  import HBasePointConsumer._

  val topic = config.getString("kafka.points.topic")
  val group = config.getString("hbase.group")

  val pair: ConsumerPair = HBasePointConsumer.createConsumer(topic, consumerConfig(config))
  if (pair.consumer.isEmpty)
    throw new ConsumerInitializationException
  val consumer = pair.consumer
  val connector = pair.connector
  lazy val maxBatchSize = config.getLong("hbase.batch-size")
  lazy val checkTick = toFiniteDuration(config.getMilliseconds("hbase.check-tick"))
  lazy val writeTimeout = toFiniteDuration(config.getMilliseconds("hbase.write-timeout"))

  var checker: Option[Cancellable] = None

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _ ⇒ Restart
    }

  override def preStart() {
    super.preStart()
    // jitter to prevent rebalance deadlock
    //context.system.scheduler.scheduleOnce(Random.nextInt(10) seconds, self, ConsumeDone(Nil))
    debug("Starting HBasePointConsumer for group " + group + " and topic " + topic)
    import context.dispatcher
    checker = Some(context.system.scheduler.schedule(checkTick, checkTick, self, CheckAvailable))
  }

  override def postStop() {
    checker foreach {
      _.cancel()
    }
    super.postStop()
    debug("Stopping HBasePointConsumer for group " + group + " and topic " + topic)
    connector.shutdown()
  }

  def receive = {
    case CheckAvailable =>
      doNext()

    case ConsumeDone(batches) =>
      connector.commitOffsets
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
    val iterator = consumer.get.iterator()
    try {
      while (iterator.hasNext && batch.size < maxBatchSize) {
        val msg: MessageAndMetadata[Array[Byte], Array[Byte]] = iterator.next()
        try {
          val (batchId, points) = PackedPoints.decodeWithBatchId(msg.message)
          metrics.batchSizeHist.update(points.size)
          batchIds += batchId
          batch ++= points
        } catch {
          case e: InvalidProtocolBufferException =>
            metrics.batchDecodeFailuresMeter.mark()
        }
      }
    } catch {
      case ex: ConsumerTimeoutException => // ok
      case ex: Throwable =>
        error("Failed to consume", ex)
        throw ex
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

  def props(config: Config, storage: PointsStorage)(implicit system: ActorSystem, metricRegistry: MetricRegistry) = {
    Props(classOf[HBasePointConsumer], config, storage, HBasePointConsumerMetrics(metricRegistry))
  }

  def start(config: Config, storage: PointsStorage)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    system.actorOf(props(config, storage)
      .withRouter(FromConfig())
      .withDispatcher("hbase.dispatcher"), "hbase-writer")
  }

  def consumerConfig(globalConfig: Config): ConsumerConfig = {
    val config: Config = globalConfig.getConfig("hbase.consumer")
    val consumerProps: Properties = config
    val timeout = globalConfig.getMilliseconds("hbase.consumer.timeout")
    consumerProps.put("consumer.timeout.ms", timeout.toString)
    consumerProps.put("group.id", globalConfig.getString("hbase.group"))
    new ConsumerConfig(consumerProps)
  }

  class ConsumerPair(val connector: ConsumerConnector, val consumer: Option[KafkaStream[Array[Byte], Array[Byte]]]) {
    def shutdown = {
      connector.shutdown()
    }
  }

  def createConsumer(topic: String, config: ConsumerConfig): ConsumerPair = {
    val consumerConnector: ConsumerConnector = Consumer.create(config)

    val topicThreadCount = Map((topic, 1))
    val topicMessageStreams = consumerConnector.createMessageStreams(topicThreadCount)
    val streams = topicMessageStreams.get(topic)

    val stream = streams match {
      case Some(List(s)) => Some(s)
      case _ => {
        error("Did not get a valid stream from topic " + topic)
        None
      }
    }
    info(s"Consumer created")
    new ConsumerPair(consumerConnector, stream)
  }
}




