package popeye.storage

import popeye.pipeline._
import com.typesafe.config.Config
import scala.concurrent.{Promise, Future, ExecutionContext}
import popeye.proto.PackedPoints
import popeye.Logging
import scala.concurrent.duration._

/**
 * @author Andrey Stepachev
 */
class BlackHole {

}

class BlackHolePipelineSinkFactory extends PipelineSinkFactory with Logging {
  def startSink(sinkName: String, channel: PipelineChannel, config: Config, storagesConfig: Config, ect: ExecutionContext): Unit = {
    implicit val ectx = ect
    val delayOption =
      if (config.hasPath("delay")) Some(config.getMilliseconds("delay"))
      else None
    channel.startReader(sinkName, new PointsSink {
      def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
        debug(s"Blackhole: ${batchIds.mkString}, ${points.pointsCount} points")
        delayOption match {
          case Some(delay) =>
            val p = Promise[Long]()
            channel.actorSystem.scheduler.scheduleOnce(delay.toInt millisecond, new Runnable {
              override def run(): Unit = p.success(points.pointsCount.toLong)
            })
            p.future
          case None =>
            Future.successful(points.pointsCount.toLong)
        }
      }
    })
  }
}

object BlackHole {
  def sinkFactory(): PipelineSinkFactory = new BlackHolePipelineSinkFactory
}
