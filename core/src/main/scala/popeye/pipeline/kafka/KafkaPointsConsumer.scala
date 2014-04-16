package popeye.pipeline.kafka

import akka.actor.{FSM, Props}
import com.codahale.metrics.MetricRegistry
import com.google.protobuf.InvalidProtocolBufferException
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import kafka.consumer.{Consumer, ConsumerConfig}
import popeye.pipeline.{PointsSource, PointsSink}
import popeye.proto.PackedPoints
import popeye.{ConfigUtil, Instrumented}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import popeye.pipeline.kafka.KafkaPointsConsumerProto._
import scala.util.Failure
import scala.Some
import scala.util.Success
import scala.concurrent.{Future, ExecutionContext}
import popeye.pipeline.kafka.KafkaPointsConsumer.DropStrategy
import popeye.proto.Message.Point

class KafkaPointsConsumerConfig(val topic: String, val group: String, config: Config) {
  val tick = new FiniteDuration(config.getMilliseconds("tick"), TimeUnit.MILLISECONDS)
  val batchSize = config.getInt("batch-size")
}

class KafkaPointsConsumerMetrics(val prefix: String,
                                 val metricRegistry: MetricRegistry) extends Instrumented {
  val emptyBatches = metrics.meter(s"$prefix.consume.empty-batches")
  val nonEmptyBatches = metrics.meter(s"$prefix.consume.non-empty-batches")
  val consumeTimer = metrics.timer(s"$prefix.consume.time")
  val consumeTimerMeter = metrics.meter(s"$prefix.consume.time-meter")
  val decodeFailures = metrics.meter(s"$prefix.consume.decode-failures")
}

object KafkaPointsConsumer {

  type DropStrategy = Seq[Point] => SendAndDrop

  def consumerConfig(group: String, kafkaConfig: Config, pc: KafkaPointsConsumerConfig): ConsumerConfig = {
    val consumerProps = ConfigUtil.mergeProperties(kafkaConfig, "consumer.config")
    consumerProps.setProperty("zookeeper.connect", kafkaConfig.getString("zk.quorum"))
    consumerProps.setProperty("metadata.broker.list", kafkaConfig.getString("broker.list"))
    consumerProps.setProperty("group.id", group)
    consumerProps.setProperty("consumer.timeout.ms", pc.tick.toMillis.toString)
    new ConsumerConfig(consumerProps)
  }

  def props(topic: String,
            group: String,
            config: Config,
            metrics: MetricRegistry,
            sink: PointsSink,
            drop: PointsSink,
            dropStrategy: DropStrategy,
            executionContext: ExecutionContext): Props = {
    val pc = new KafkaPointsConsumerConfig(topic, group, config.getConfig("consumer").withFallback(config))
    val consumerMetrics = new KafkaPointsConsumerMetrics(s"kafka.$topic.$group", metrics)
    val consumerConfig = KafkaPointsConsumer.consumerConfig(group, config, pc)
    val consumerConnector = Consumer.create(consumerConfig)
    val sourceMetrics = new KafkaPointsSourceImplMetrics(f"${consumerMetrics.prefix}.source", metrics)
    val pointsSource = new KafkaPointsSourceImpl(consumerConnector, pc.topic, sourceMetrics)
    Props.apply(
      new KafkaPointsConsumer(
        pc,
        consumerMetrics,
        pointsSource,
        sink,
        drop,
        dropStrategy,
        executionContext
      )
    )
  }
}

object KafkaPointsConsumerProto {

  sealed trait Cmd
  case object Ok extends Cmd
  case object Failed extends Cmd

  sealed trait State

  case object Idle extends State
  case object Consuming extends State
  case object Delivering extends State

  case class PointsState(pointsToSend: PackedPoints = PackedPoints(),
                         pointsToDrop: PackedPoints = PackedPoints(),
                         batches: ArrayBuffer[Long] = new ArrayBuffer[Long]()) {
    def hasData = pointsCount > 0

    def pointsCount = pointsToSend.size + pointsToDrop.size
  }

}

case class SendAndDrop(pointsToSend: Seq[Point] = Seq(),
                       pointsToDrop: Seq[Point] = Seq())

class KafkaPointsConsumer(val config: KafkaPointsConsumerConfig,
                          val metrics: KafkaPointsConsumerMetrics,
                          val pointsConsumer: PointsSource,
                          val sinkPipe: PointsSink,
                          val dropPipe: PointsSink,
                          dropStrategy: DropStrategy,
                          executionContext: ExecutionContext)
  extends FSM[State, PointsState] {

  implicit val eCtx = executionContext

  startWith(Idle, PointsState())

  when(Idle, stateTimeout = config.tick) {
    case Event(StateTimeout, _) =>
      goto(Consuming)
  }

  when(Consuming, stateTimeout = FiniteDuration.apply(0, TimeUnit.MILLISECONDS)) {
    case Event(StateTimeout, p: PointsState) =>
      consumeNext(stateData, config.batchSize)
      log.debug(s"consume: buffered ${stateData.pointsCount} points")
      if (p.hasData) {
        metrics.nonEmptyBatches.mark()
        deliverPoints()
        goto(Delivering)
      }
      else {
        metrics.emptyBatches.mark()
        goto(Idle) using PointsState()
      }
  }

  when(Delivering) {
    case Event(Ok, p: PointsState) =>
      log.info(s"Committing batches ${p.batches.mkString}")
      pointsConsumer.commitOffsets()
      goto(Consuming) using PointsState()

    case Event(Failed, p: PointsState) =>
      log.info("Delivery failed: terminating")
      context.stop(self)
      goto(Idle) using PointsState()
  }

  private def deliverPoints() {
    val myBatches = stateData.batches
    val sendFuture = sendPoints(myBatches, stateData.pointsToSend)
    val dropFuture = dropPoints(myBatches, stateData.pointsToDrop)
    (sendFuture zip dropFuture) onComplete {
      case Success(x) =>
        log.debug(s"Batches: ${myBatches.mkString(",")} committed")
        self ! Ok
      case Failure(x: Throwable) =>
        log.error(x, s"Failed to deliver batches $myBatches")
        self ! Failed
    }
  }

  private def sendPoints(batchIds: Seq[Long], points: PackedPoints) = {
    if (points.nonEmpty) {
      log.debug(s"Sending ${batchIds.mkString(", ")} with ${stateData.pointsCount} points")
      val future = sinkPipe.send(batchIds, points)
      future.recoverWith {
        case t: Throwable =>
          log.error(t, s"Failed to send batches $batchIds")
          dropPipe.send(batchIds, points)
      }
    } else {
      Future.successful(0l)
    }
  }

  private def dropPoints(batchIds: Seq[Long], points: PackedPoints) = {
    if (points.nonEmpty) {
      log.debug(s"Dropping ${stateData.batches.mkString} with ${stateData.pointsCount} points")
      val future = dropPipe.send(batchIds, points)
      future.onComplete {
        case Success(x) =>
          log.debug(s"Batches: $batchIds successfully dropped")
        case Failure(x: Throwable) =>
          log.error(s"Batches: $batchIds failed to safe drop", x)
      }
      future
    } else {
      Future.successful(0l)
    }
  }

  private def consumeNext(p: PointsState, batchSize: Long): Unit = {
    val tctx = metrics.consumeTimer.timerContext()
    try {
      consumeInner(p, batchSize)
    } finally {
      val time = tctx.stop().nano
      metrics.consumeTimerMeter.mark(time.toMillis)
    }
  }

  private def consumeInner(p: PointsState, batchSize: Long): Unit = {
    while(p.pointsCount < batchSize) {
      try {
        pointsConsumer.consume() match {
          case Some((batchId, points)) =>
            log.debug(s"Batch: $batchId queued")
            val SendAndDrop(sendPoints, dropPoints) = dropStrategy(points)
            p.pointsToSend.append(sendPoints: _*)
            p.pointsToDrop.append(dropPoints: _*)
            p.batches += batchId
          case None =>
            return
        }
      } catch {
        case e: InvalidProtocolBufferException =>
          log.debug("Can't decode point", e)
          metrics.decodeFailures.mark()
      }
    }
  }

  initialize()
}
