package popeye.storage

import popeye.pipeline._
import com.typesafe.config.Config
import scala.concurrent.{Promise, Future, ExecutionContext}
import akka.actor.Props
import popeye.proto.PackedPoints
import popeye.Logging

/**
 * @author Andrey Stepachev
 */
class BlackHole {

}

class BlackHolePipelineSinkFactory extends PipelineSinkFactory with Logging {
  def startSink(sinkName: String, channel: PipelineChannel, config: Config, storagesConfig: Config, ect: ExecutionContext): Unit = {
    channel.startReader(sinkName, new PointsSink {
      def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
        debug(s"Blackhole: ${batchIds.mkString}, ${points.pointsCount} points")
        Promise.successful(points.pointsCount.toLong).future
      }
    })
  }
}

object BlackHole {
  def sinkFactory(): PipelineSinkFactory = new BlackHolePipelineSinkFactory
}
