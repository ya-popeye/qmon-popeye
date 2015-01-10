package popeye.storage

import popeye.pipeline._
import com.typesafe.config.Config
import scala.concurrent.{Promise, Future, ExecutionContext}
import akka.actor.ActorSystem
import popeye.proto.PackedPoints
import popeye.Logging
import scala.concurrent.duration._
import popeye.proto.Message

/**
 * @author Andrey Stepachev
 */
class BlackHole {

}

class BlackHolePipelineSinkFactory(actorSystem: ActorSystem,
                                   ectx: ExecutionContext) extends PipelineSinkFactory with Logging {
  def startSink(sinkName: String, config: Config): PointsSink = {
    implicit val ect = ectx
    val delayOption =
      if (config.hasPath("delay")) Some(config.getMilliseconds("delay"))
      else None

    new PointsSink {
      override def sendPoints(batchId: Long, points: Message.Point*): Future[Long] = {
        doBlackHole(batchId, points.length)
      }

      override def sendPacked(batchId: Long, buffers: PackedPoints*): Future[Long] = {
        val points = buffers.foldLeft(0) { (a, b) => a + b.pointsCount}
        doBlackHole(batchId, points)
      }

      def doBlackHole(batchId: Long, count: Long) = {
        debug(s"Blackhole: $batchId, $count points")
        delayOption match {
          case Some(delay) =>
            val p = Promise[Long]()
            actorSystem.scheduler.scheduleOnce(delay.toInt millisecond, new Runnable {
              override def run(): Unit = p.success(count.toLong)
            })
            p.future
          case None =>
            Future.successful(count.toLong)
        }
      }

      override def close(): Unit = {}
    }
  }
}
