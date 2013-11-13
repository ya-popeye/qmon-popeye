package popeye.pipeline.kafka

import akka.actor.{Props, Actor}
import com.google.protobuf.InvalidProtocolBufferException
import popeye.{ConfigUtil, Instrumented, Logging}
import popeye.proto.PackedPoints
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}
import popeye.pipeline.{PointsSource, PointsSink}
import com.typesafe.config.Config
import com.codahale.metrics.MetricRegistry
import kafka.consumer.{Consumer, ConsumerConfig}

class KafkaPointsConsumerConfig(config: Config) {
  val topic = config.getString("topic")
  val group = config.getString("group")
  val batchSize = config.getInt("batch-size")
  val maxLag = config.getMilliseconds("max-lag")
}

class KafkaPointsConsumerMetrics(val prefix: String,
                                 val metricRegistry: MetricRegistry) extends Instrumented {
  val consumeTimer = metrics.timer(s"$prefix.consume.time")
  val decodeFailures = metrics.meter(s"$prefix.consume.decode-failures")
}

private object KafkaPointsConsumerProto {
  case class NextChunk()

  case class FailedChunk()

  case class CompletedChunk()
}

object KafkaPointsConsumer {
  def consumerConfig(group: String, kafkaConfig: Config): ConsumerConfig = {
    val consumerProps = ConfigUtil.mergeProperties(kafkaConfig, "consumer.config")
    consumerProps.setProperty("zookeeper.connect", kafkaConfig.getString("zk.quorum"))
    consumerProps.setProperty("metadata.broker.list", kafkaConfig.getString("broker.list"))
    consumerProps.setProperty("group.id", group)
    new ConsumerConfig(consumerProps)
  }

  def props(group: String, config: Config, metrics: MetricRegistry, sink: PointsSink, drop: PointsSink): Props = {
    val pc = new KafkaPointsConsumerConfig(config.getConfig("consumer").withFallback(config))
    val m = new KafkaPointsConsumerMetrics(s"kafka.$group", metrics)
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

  override def preStart(): Unit = {
    super.preStart()
    self ! NextChunk
  }

  def receive: Actor.Receive = {

    case NextChunk =>
      tryCommit()
      consumeNext(buffer)
      if (buffer.pointsCount > config.batchSize) {
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
      } else {
        self ! NextChunk
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
        metrics.decodeFailures.mark()
    }
    tctx.close()
  }
}
