package popeye.pipeline.kafka

import akka.actor.{FSM, Props}
import com.codahale.metrics.MetricRegistry
import com.google.protobuf.InvalidProtocolBufferException
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import kafka.consumer.{Consumer, ConsumerConfig}
import popeye.pipeline.{PointsSource, PointsSink}
import popeye.proto.PackedPoints
import popeye.{IdGenerator, ConfigUtil, Instrumented}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Future, ExecutionContext}
import java.util.concurrent.atomic.AtomicInteger
import popeye.pipeline.kafka.KafkaPointsConsumerProto._
import popeye.pipeline.kafka.KafkaPointsConsumer.DropStrategy
import popeye.pipeline.kafka.KafkaPointsConsumerProto.ConsumerState
import scala.util.Failure
import scala.Some
import scala.util.Success

case class KafkaPointsConsumerConfig(tick: FiniteDuration,
                                     backoff: FiniteDuration,
                                     batchSize: Int,
                                     maxParallelSenders: Int)


object KafkaPointsConsumerConfig {

  def apply(config: Config): KafkaPointsConsumerConfig = {
    KafkaPointsConsumerConfig(
      tick = new FiniteDuration(config.getMilliseconds("tick"), TimeUnit.MILLISECONDS),
      backoff = new FiniteDuration(config.getMilliseconds("backoff"), TimeUnit.MILLISECONDS),
      batchSize = config.getInt("batch-size"),
      maxParallelSenders = config.getInt("max-parallel-senders")
    )
  }
}

object KafkaConsumerConfig {

  def apply(group: String, config: Config): ConsumerConfig = {
    val tick = new FiniteDuration(config.getMilliseconds("tick"), TimeUnit.MILLISECONDS)
    val zkQuorum = config.getString("zk.quorum")
    val brokerList = config.getString("broker.list")
    val consumerProperties = ConfigUtil.mergeProperties(config, "config")
    consumerProperties.setProperty("zookeeper.connect", zkQuorum)
    consumerProperties.setProperty("metadata.broker.list", brokerList)
    consumerProperties.setProperty("consumer.timeout.ms", tick.toMillis.toString)
    consumerProperties.setProperty("group.id", group)
    new ConsumerConfig(consumerProperties)
  }
}

class KafkaPointsConsumerMetrics(val prefix: String,
                                 val metricRegistry: MetricRegistry) extends Instrumented {
  val emptyBatches = metrics.meter(s"$prefix.consume.empty-batches")
  val nonEmptyBatches = metrics.meter(s"$prefix.consume.non-empty-batches")
  val consumeTimer = metrics.timer(s"$prefix.consume.time")
  val consumeTimerMeter = metrics.meter(s"$prefix.consume.time-meter")
  val decodeFailures = metrics.meter(s"$prefix.consume.decode-failures")
  val deliveredPoints = metrics.meter(s"$prefix.consume.points")

  def addSenderNumberGauge(numberOfSenders: => Int) = {
    metrics.gauge(s"$prefix.consume.senders") {
      numberOfSenders
    }
  }
}

object KafkaConsumer {

  def consumerConfig(group: String, consumerTimeout: FiniteDuration, kafkaConfig: Config): ConsumerConfig = {
    val consumerProps = ConfigUtil.mergeProperties(kafkaConfig, "consumer.config")
    consumerProps.setProperty("zookeeper.connect", kafkaConfig.getString("zk.quorum"))
    consumerProps.setProperty("metadata.broker.list", kafkaConfig.getString("broker.list"))
    consumerProps.setProperty("group.id", group)
    consumerProps.setProperty("consumer.timeout.ms", consumerTimeout.toMillis.toString)
    new ConsumerConfig(consumerProps)
  }
}

object KafkaPointsConsumer {

  type DropStrategy = PackedPoints => SendAndDrop

  def props(name: String,
            config: KafkaPointsConsumerConfig,
            metrics: MetricRegistry,
            pointsSource: PointsSource,
            sink: PointsSink,
            drop: PointsSink,
            dropStrategy: DropStrategy,
            idGenerator: IdGenerator,
            executionContext: ExecutionContext): Props = {
    val consumerMetrics = new KafkaPointsConsumerMetrics(name, metrics)
    Props.apply(new KafkaPointsConsumer(config,
      consumerMetrics, pointsSource, sink, drop, dropStrategy,
      idGenerator, executionContext))
  }
}

object KafkaPointsConsumerProto {

  case object Ok

  case class Failed(cause: Throwable, failedBatches: PointBatches)

  case object Consume

  case class TryToRedeliver(failedBatches: PointBatches)

  sealed trait State

  case object Idle extends State

  case object Working extends State

  case class ConsumerState(startedSenders: Int = 0,
                           completedDeliveries: Int = 0,
                           batchIds: ArrayBuffer[Long] = ArrayBuffer(),
                           sentPointsCount: Int = 0)

  case class PointBatches(pointsToSend: PackedPoints,
                          pointsToDrop: PackedPoints,
                          batchIds: Seq[Long]) {
    def hasData = pointsCount > 0

    def pointsCount = pointsToSend.size + pointsToDrop.size
  }

}

case class SendAndDrop(pointsToSend: PackedPoints = PackedPoints(),
                       pointsToDrop: PackedPoints = PackedPoints())

class KafkaPointsConsumer(val config: KafkaPointsConsumerConfig,
                          val metrics: KafkaPointsConsumerMetrics,
                          val pointsConsumer: PointsSource,
                          val sinkPipe: PointsSink,
                          val dropPipe: PointsSink,
                          dropStrategy: DropStrategy,
                          idGenerator: IdGenerator,
                          executionContext: ExecutionContext)
  extends FSM[State, ConsumerState] {

  implicit val eCtx = executionContext
  val numberOfSenders = new AtomicInteger(0)
  metrics.addSenderNumberGauge {
    numberOfSenders.get
  }

  val failoverSink = new FailoverSink(sinkPipe, dropPipe, dropStrategy, log)

  startWith(Idle, ConsumerState())

  when(Idle, stateTimeout = config.tick) {
    case Event(StateTimeout, state) =>
      self ! Consume
      goto(Working)
  }

  when(Working) {
    case Event(Consume, state) =>
      val points = consumeNext(config.batchSize)
      log.debug(s"consume: buffered ${points.pointsCount} points")
      if (points.hasData) {
        metrics.nonEmptyBatches.mark()
        if (state.startedSenders + 1 < config.maxParallelSenders) {
          self ! Consume
        }
        deliverPoints(points)
        numberOfSenders.incrementAndGet()
        stay() using state.copy(
          startedSenders = state.startedSenders + 1,
          batchIds = state.batchIds ++ points.batchIds,
          sentPointsCount = state.sentPointsCount + points.pointsCount
        )
      } else if (state.completedDeliveries < state.startedSenders) {
        stay()
      } else {
        metrics.emptyBatches.mark()
        goto(Idle) using ConsumerState()
      }

    case Event(Ok, state) =>
      numberOfSenders.decrementAndGet()
      // if all senders succeeded then offsets can be safely committed
      val completedDeliveries = state.completedDeliveries + 1
      if (completedDeliveries == state.startedSenders) {
        log.info(s"Committing batches ${state.batchIds}")
        pointsConsumer.commitOffsets()
        self ! Consume
        metrics.deliveredPoints.mark(state.sentPointsCount)
        stay() using ConsumerState()
      } else {
        stay() using state.copy(completedDeliveries = completedDeliveries)
      }

    case Event(Failed(cause, points), _) =>
      log.error(cause, "Delivery failed, retrying to send batches {}", points.batchIds)
      context.system.scheduler.scheduleOnce(config.backoff) {
        self ! TryToRedeliver(points)
      }
      stay()

    case Event(TryToRedeliver(points), _) =>
      deliverPoints(points)
      stay()
  }

  private def deliverPoints(points: PointBatches) {
    val batchId = if (points.batchIds.size == 1) {
      points.batchIds.head
    } else {
      idGenerator.nextId()
    }
    failoverSink.sendBatches(batchId, points) onComplete {
      case Success(x) =>
        log.debug(s"Batches: ${ points.batchIds } delivered as $batchId")
        self ! Ok
      case Failure(x: Throwable) =>
        log.error(x, s"Failed to deliver batches ${ points.batchIds }")
        self ! Failed(x, points)
    }
  }

  private def consumeNext(batchSize: Long): PointBatches = {
    val tctx = metrics.consumeTimer.timerContext()
    try {
      consumeInner(batchSize)
    } finally {
      val time = tctx.stop().nano
      metrics.consumeTimerMeter.mark(time.toMillis)
    }
  }

  private def consumeInner(batchSize: Long): PointBatches = {
    val pointsToSend = PackedPoints()
    val pointsToDrop = PackedPoints()
    val batches = ArrayBuffer[Long]()
    while(pointsToSend.size + pointsToDrop.size < batchSize) {
      try {
        pointsConsumer.consume() match {
          case Some((batchId, points)) =>
            log.debug(s"Batch: $batchId queued")
            val SendAndDrop(send, drop) = dropStrategy(points)
            pointsToSend.consumeFrom(send, send.pointsCount)
            pointsToDrop.consumeFrom(drop, drop.pointsCount)
            batches += batchId
          case None =>
            return PointBatches(pointsToSend, pointsToDrop, batches.toList)
        }
      } catch {
        case e: InvalidProtocolBufferException =>
          log.debug("Can't decode point", e)
          metrics.decodeFailures.mark()
      }
    }
    PointBatches(pointsToSend, pointsToDrop, batches.toList)
  }

  initialize()
}

class FailoverSink(sinkPipe: PointsSink,
                   dropPipe: PointsSink,
                   dropStrategy: DropStrategy,
                   log: akka.event.LoggingAdapter)
                  (implicit ectx: ExecutionContext) {

  def sendBatches(batchId: Long, points: PointBatches): Future[(Long, Long)] = {
    val sendFuture = sendPoints(batchId, points.pointsToSend)
    val dropFuture = dropPoints(batchId, points.batchIds, points.pointsToDrop)
    sendFuture zip dropFuture
  }

  private def sendPoints(batchId: Long, points: PackedPoints) = {
    if (points.nonEmpty) {
      log.debug(s"Sending $batchId with ${ points.pointsCount } points")
      val future = sinkPipe.sendPacked(batchId, points)
      future.recoverWith {
        case t: Throwable =>
          log.error(t, s"Failed to send batches $batchId")
          dropPipe.sendPacked(batchId, points)
      }
    } else {
      Future.successful(0l)
    }
  }

  private def dropPoints(batchId: Long, batchIds: Seq[Long], points: PackedPoints) = {
    if (points.nonEmpty) {
      log.debug(s"Dropping $batchId with ${ points.pointsCount } points as batch $batchId")
      val future = dropPipe.sendPacked(batchId, points)
      future.onComplete {
        case Success(x) =>
          log.debug(s"Batches: $batchIds successfully dropped as $batchId")
        case Failure(x: Throwable) =>
          log.error(s"Batches: $batchIds failed to safe drop", x)
      }
      future
    } else {
      Future.successful(0l)
    }
  }
}
