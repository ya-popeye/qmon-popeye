package popeye.pipeline.kafka

import akka.actor.{Cancellable, Props, Actor}
import com.codahale.metrics.MetricRegistry
import com.google.protobuf.InvalidProtocolBufferException
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import kafka.consumer.{Consumer, ConsumerConfig}
import popeye.pipeline.{PointsSource, PointsSink}
import popeye.proto.PackedPoints
import popeye.{ConfigUtil, Instrumented, Logging}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class KafkaPointsConsumerConfig(val topic: String, val group: String, config: Config) {
  val tick = new FiniteDuration(config.getMilliseconds("tick"), TimeUnit.MILLISECONDS)
  val batchSize = config.getInt("batch-size")
  val maxLag = config.getMilliseconds("max-lag")
}

class KafkaPointsConsumerMetrics(val prefix: String,
                                 val metricRegistry: MetricRegistry) extends Instrumented {
  val consumeTimer = metrics.timer(s"$prefix.consume.time")
  val decodeFailures = metrics.meter(s"$prefix.consume.decode-failures")
}

private object KafkaPointsConsumerProto {

  case object Tick

  case object NextChunk

  case object FailedChunk

  case object CompletedChunk

}

object KafkaPointsConsumer {
  def consumerConfig(group: String, kafkaConfig: Config): ConsumerConfig = {
    val consumerProps = ConfigUtil.mergeProperties(kafkaConfig, "consumer.config")
    consumerProps.setProperty("zookeeper.connect", kafkaConfig.getString("zk.quorum"))
    consumerProps.setProperty("metadata.broker.list", kafkaConfig.getString("broker.list"))
    consumerProps.setProperty("group.id", group)
    new ConsumerConfig(consumerProps)
  }

  def props(topic: String, group: String, config: Config, metrics: MetricRegistry, sink: PointsSink, drop: PointsSink): Props = {
    val pc = new KafkaPointsConsumerConfig(topic, group, config.getConfig("consumer").withFallback(config))
    val m = new KafkaPointsConsumerMetrics(s"kafka.$topic.$group", metrics)
    val consumerConfig = KafkaPointsConsumer.consumerConfig(group, config)
    val consumerConnector = Consumer.create(consumerConfig)
    val pointsSource = new KafkaPointsSourceImpl(consumerConnector, pc.topic)
    Props.apply(new KafkaPointsConsumer(pc, m, pointsSource, sink, drop))
  }
}

class KafkaPointsConsumer(val config: KafkaPointsConsumerConfig,
                          val metrics: KafkaPointsConsumerMetrics,
                          val pointsConsumer: PointsSource,
                          val sinkPipe: PointsSink,
                          val dropPipe: PointsSink)
  extends Actor with Logging {

  import KafkaPointsConsumerProto._
  import scala.concurrent.ExecutionContext.Implicits.global

  private var buffer = new PackedPoints()
  private var batches = new ArrayBuffer[Long]()
  private var commit = false
  private var flush = false

  override def preStart(): Unit = {
    super.preStart()
    self ! NextChunk
  }

  override def postStop(): Unit = {
    info("Consumer stopped")
    super.postStop()
  }

  def receive: Actor.Receive = {

    case Tick =>
      flush = true
      self ! NextChunk

    case NextChunk =>
      debug(s"Buffered ${buffer.pointsCount} points so far (flush = $flush)")
      tryCommit()
      consumeNext(buffer)
      debug(s"Collected ${buffer.pointsCount} points so far (flush = $flush)")
      if ( flush || (buffer.pointsCount > config.batchSize) ) {
        val me = self
        val myBatches = batches
        sinkPipe.send(myBatches, buffer) onComplete {
          case Success(x) =>
            commit = true
            debug(s"Batches: ${myBatches.mkString(",")} commited")

            me ! NextChunk
          case Failure(x: Throwable) =>
            error(s"Failed to send batches $myBatches", x)
            me ! FailedChunk
        }
        flush = false
      } else {
        context.system.scheduler.scheduleOnce(config.tick, self, Tick)
      }

    case FailedChunk =>
      val me = self
      dropPipe.send(batches, buffer) onComplete {
        case Success(x) =>
          commit = true
          me ! NextChunk
        case Failure(x) =>
          error("fatal, can't drop ")
      }
  }

  private def tryCommit() = {
    if (commit) {
      debug(s"Commiting offsets: affected batches ${batches.mkString}")
      buffer = new PackedPoints
      batches = new ArrayBuffer[Long]()
      commit = false
      pointsConsumer.commitOffsets()
    }
  }

  private def consumeNext(buffer: PackedPoints): Unit = {
    val tctx = metrics.consumeTimer.timerContext()
    try {
      pointsConsumer.consume() match {
        case Some((batchId, points)) =>
          debug(s"Batch: $batchId queued")
          buffer.append(points: _*)
          batches += batchId

        case None =>

      }
    } catch {
      case e: InvalidProtocolBufferException =>
        debug("Can't decode point", e)
        metrics.decodeFailures.mark()
    }
    tctx.close()
  }
}
