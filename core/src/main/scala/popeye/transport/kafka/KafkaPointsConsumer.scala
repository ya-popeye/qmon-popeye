package popeye.transport.kafka

import com.google.protobuf.InvalidProtocolBufferException
import com.typesafe.config.Config
import popeye.transport.proto.PackedPoints
import popeye.{IdGenerator, Instrumented, Logging}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success}

trait KafkaConsumerMetrics extends Instrumented {
  def prefix: String

  val consumeTimer = metrics.timer(s"$prefix.consume.time")
  val decodeFailures = metrics.meter(s"$prefix.consume.decode-failures")
}

trait KafkaConsumerConfig {
  def config: Config

  val topic = config.getString("topic")
  val group = config.getString("group")
  val batchSize = config.getInt("batch-size")
  val queueSize = config.getInt("queue-size")
  val maxLag = config.getMilliseconds("max-lag")
}

trait PointsPipeline {
  def send(batchId: Long, points: PackedPoints): Future[Long]
}

sealed case class TimedPointsBatch(points: PackedPoints, time: Long = System.currentTimeMillis())

trait KafkaPointsConsumer extends Logging {
  type Metrics <: KafkaConsumerMetrics
  type Config <: KafkaConsumerConfig
  type Consumer <: PopeyeKafkaConsumer

  def metrics: Metrics

  def config: Config

  def idGenerator: IdGenerator

  def pointsConsumer: Consumer

  def sinkPipe: PointsPipeline

  def dropPipe: PointsPipeline

  private val queue = ArrayBuffer[TimedPointsBatch]()

  private def doWork() = {
    if (queue.size <= config.queueSize) {
      consumeNext()
    }
    if (!queue.isEmpty) {
      processQueue()
    }
  }

  private def processQueue() = {
  }

  private def consumeNext(): Unit = {
    val tctx = metrics.consumeTimer.timerContext()
    try {
      pointsConsumer.consume() match {
        case Some((batchId, points)) =>
          debug(s"Batch: $batchId queued")
          queue.append(TimedPointsBatch(PackedPoints(points)))

        case None =>

      }
    } catch {
      case e: InvalidProtocolBufferException =>
        metrics.decodeFailures.mark()
    }
    tctx.close()
  }
}
