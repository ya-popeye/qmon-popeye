package popeye.storage

import popeye.pipeline._
import com.typesafe.config.Config
import scala.concurrent.{Promise, Future, ExecutionContext}
import akka.actor.{ActorSystem, Props}
import popeye.proto.PackedPoints
import popeye.Logging
import scala.concurrent.duration._
import com.codahale.metrics.MetricRegistry

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
      def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
        debug(s"Blackhole: ${batchIds.mkString}, ${points.pointsCount} points")
        delayOption match {
          case Some(delay) =>
            val p = Promise[Long]()
            actorSystem.scheduler.scheduleOnce(delay.toInt millisecond, new Runnable {
              override def run(): Unit = p.success(points.pointsCount.toLong)
            })
            p.future
          case None =>
            Future.successful(points.pointsCount.toLong)
        }
      }
    }
  }
}