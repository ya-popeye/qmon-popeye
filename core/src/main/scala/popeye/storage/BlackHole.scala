package popeye.storage

import popeye.pipeline._
import com.typesafe.config.Config
import scala.concurrent.{Future, ExecutionContext}
import akka.actor.Props
import popeye.proto.PackedPoints

/**
 * @author Andrey Stepachev
 */
class BlackHole {

}

class BlackHolePipelineSinkFactory extends PipelineSinkFactory {
  def startSink(sinkName: String, channel: PipelineChannel, config: Config, ect: ExecutionContext): Unit = {
    val dc = new PointsDispatcherConfig(config)
    val m = new PointsDispatcherMetrics(s"blackhole.$sinkName", channel.metrics)
    val f = new PointsSinkFactory {
      def newPointsSink(): PointsSink = new PointsSink {
        def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
          Future[Long] { points.pointsCount } (ect)
        }
      }
    }
    channel.actorSystem.actorOf(Props.apply(
      new PointsSinkDispatcherActor(dc, channel.idGenerator, f, m)
    ))
  }
}

object BlackHole {
  def sinkFactory(): PipelineSinkFactory = new BlackHolePipelineSinkFactory
}
