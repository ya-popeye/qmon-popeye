package popeye.transport.kafka

import akka.actor.Actor
import com.google.protobuf.InvalidProtocolBufferException
import popeye.Logging
import popeye.transport.proto.{PackedPoints}
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}
import popeye.pipeline.PointsSink

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
