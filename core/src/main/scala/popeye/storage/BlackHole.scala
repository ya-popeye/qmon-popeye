package popeye.storage

import popeye.pipeline._
import com.typesafe.config.Config
import scala.concurrent.{Promise, Future}
import popeye.proto.PackedPoints
import popeye.Logging

/**
 * @author Andrey Stepachev
 */
class BlackHole {

}

class BlackHolePipelineSinkFactory extends PipelineSinkFactory with Logging {
  def startSink(sinkName: String, config: Config): PointsSink = {
    new PointsSink {
      def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
        debug(s"Blackhole: $sinkName ${batchIds.mkString}, ${points.pointsCount} points")
        Promise.successful(points.pointsCount.toLong).future
      }
    }
  }
}

object BlackHole {
  def sinkFactory(): PipelineSinkFactory = new BlackHolePipelineSinkFactory
}
