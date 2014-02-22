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
import scala.concurrent.duration.FiniteDuration
import popeye.pipeline.kafka.KafkaPointsConsumerProto._
import scala.util.Failure
import scala.Some
import scala.util.Success
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext

class KafkaPointsConsumerConfig(val topic: String, val group: String, config: Config) {
  val tick = new FiniteDuration(config.getMilliseconds("tick"), TimeUnit.MILLISECONDS)
  val batchSize = config.getInt("batch-size")
}

class KafkaPointsConsumerMetrics(val prefix: String,
                                 val metricRegistry: MetricRegistry) extends Instrumented {
  val consumeTimer = metrics.timer(s"$prefix.consume.time")
  val decodeFailures = metrics.meter(s"$prefix.consume.decode-failures")
}

object KafkaPointsConsumer {
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
            executionContext: ExecutionContext): Props = {
    val pc = new KafkaPointsConsumerConfig(topic, group, config.getConfig("consumer").withFallback(config))
    val m = new KafkaPointsConsumerMetrics(s"kafka.$topic.$group", metrics)
    val consumerConfig = KafkaPointsConsumer.consumerConfig(group, config, pc)
    val consumerConnector = Consumer.create(consumerConfig)
    val pointsSource = new KafkaPointsSourceImpl(consumerConnector, pc.topic)
    Props.apply(new KafkaPointsConsumer(pc, m, pointsSource, sink, drop, executionContext))
  }
}

object KafkaPointsConsumerProto {

  sealed trait Cmd
  case object Ok extends Cmd
  case object Failed extends Cmd

  sealed trait State

  case object Idle extends State
  case object Active extends State
  case object Sending extends State
  case object Dropping extends State

  case class PointsState(var points: PackedPoints = new PackedPoints(),
                   var batches: ArrayBuffer[Long] = new ArrayBuffer[Long]()){
    def hasData = points.pointsCount > 0
  }
}

class KafkaPointsConsumer(val config: KafkaPointsConsumerConfig,
                          val metrics: KafkaPointsConsumerMetrics,
                          val pointsConsumer: PointsSource,
                          val sinkPipe: PointsSink,
                          val dropPipe: PointsSink,
                          executionContext: ExecutionContext)
  extends FSM[State, PointsState] {

  implicit val eCtx = executionContext

  startWith(Idle, PointsState())

  when(Idle, stateTimeout = config.tick) {
    case Event(StateTimeout, _) =>
      goto(Active)
  }

  when(Active, stateTimeout = config.tick) {
    case Event(StateTimeout, p: PointsState) =>
      log.debug(s"Tick: ${p.batches.mkString}")
      if (p.hasData)
        goto(Sending)
      else
        goto(Idle) using PointsState()
  }

  when(Sending) {
    case Event(Ok, p: PointsState) =>
      log.info(s"Committing batches ${p.batches.mkString}")
      pointsConsumer.commitOffsets()
      goto(Active) using PointsState()

    case Event(Failed, p: PointsState) =>
      log.info(s"Failed batches ${p.batches.mkString}, sending to dropPipe")
      goto(Dropping)
  }

  when(Dropping) {
    case Event(Ok, _) =>
      pointsConsumer.commitOffsets()
      goto(Active) using PointsState()
    case Event(Failed, _) =>
      log.info("Drop failed: terminating")
      context.stop(self)
      goto(Idle) using PointsState()
  }

  onTransition {
    case Idle -> Active =>
      consumeNext(stateData, config.batchSize)
      log.debug(s"consume: buffered ${stateData.points.pointsCount} points")

    case Active -> Sending =>
      log.debug(s"Sending ${stateData.batches.mkString} with ${stateData.points.pointsCount} points")
      sendPoints(stateData)

    case Sending -> Dropping =>
      log.debug(s"Droping ${stateData.batches.mkString} with ${stateData.points.pointsCount} points")
      dropPoints(stateData)

  }

  private def sendPoints(p: PointsState) = {
    val me = self
    val myBatches = p.batches
    sinkPipe.send(myBatches, p.points) onComplete {
      case Success(x) =>
        log.debug(s"Batches: ${myBatches.mkString(",")} committed")
        me ! Ok
      case Failure(x: Throwable) =>
        log.error(x, s"Failed to send batches $myBatches")
        me ! Failed
    }
  }

  private def dropPoints(p: PointsState) = {
    val me = self
    val batches = p.batches.mkString(",")
    dropPipe.send(p.batches, p.points) onComplete {
      case Success(x) =>
        log.debug(s"Batches: $batches successfully dropped")
        me ! Ok
      case Failure(x: Throwable) =>
        log.error(s"Batches: $batches failed to safe drop", x)
        me ! Failed
    }

  }


  private def consumeNext(p: PointsState, batchSize: Long): Unit = {
    val tctx = metrics.consumeTimer.timerContext()
    try {
      consumeInner(p, batchSize)
    } finally {
      tctx.close()
    }
  }

  @tailrec
  private def consumeInner(p: PointsState, batchSize: Long): Unit = {
    if (p.points.pointsCount > batchSize)
      return
    try {
      pointsConsumer.consume() match {
        case Some((batchId, points)) =>
          log.debug(s"Batch: $batchId queued")
          p.points.append(points: _*)
          p.batches += batchId

        case None =>
          return
      }
    } catch {
      case e: InvalidProtocolBufferException =>
        log.debug("Can't decode point", e)
        metrics.decodeFailures.mark()
    }
    consumeInner(p, batchSize)
  }

  initialize()
}
