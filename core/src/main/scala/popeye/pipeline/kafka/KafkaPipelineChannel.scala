package popeye.pipeline.kafka

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.IdGenerator
import popeye.pipeline._
import popeye.proto.PackedPoints
import scala.concurrent.Future

/**
 * @author Andrey Stepachev
 */
class KafkaPipelineChannel(val config: Config,
                           val actorSystem: ActorSystem,
                           val metrics: MetricRegistry,
                           val idGenerator: IdGenerator)
  extends PipelineChannel {

  def newWriter(): PipelineChannelWriter = ???

  def startReader(group: String, sink: PointsSink): Unit = {
    actorSystem.actorOf(KafkaPointsConsumer.props(group, config, metrics, sink, new PointsSink {
      def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
        throw new IllegalStateException("Drop not enabled")
      }
    }))
  }
}
