package popeye.transport.kafka

import akka.actor.Actor
import com.google.protobuf.InvalidProtocolBufferException
import com.typesafe.config.Config
import popeye.transport.proto.PackedPoints
import popeye.{Instrumented, Logging}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success}
import com.codahale.metrics.MetricRegistry

class KafkaPointsConsumerMetrics(val prefix: String,
                                 val metricRegistry: MetricRegistry) extends Instrumented {
  val consumeTimer = metrics.timer(s"$prefix.consume.time")
  val decodeFailures = metrics.meter(s"$prefix.consume.decode-failures")
}

class KafkaPointsConsumerConfig(config: Config) {
  val topic = config.getString("topic")
  val group = config.getString("group")
  val batchSize = config.getInt("batch-size")
  val maxLag = config.getMilliseconds("max-lag")
}

trait PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long]
}

private object KafkaPointsConsumerProto {
  case class NextChunk()

  case class FailedChunk()

  case class CompletedChunk()
}

class KafkaPointsConsumer(val config: KafkaPointsConsumerConfig,
                          val metrics: KafkaPointsConsumerMetrics,
                          val pointsConsumer: PopeyeKafkaConsumer,
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
