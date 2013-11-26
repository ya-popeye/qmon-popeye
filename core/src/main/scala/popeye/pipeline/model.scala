package popeye.pipeline
import scala.concurrent.{Promise, ExecutionContext, Future}
import java.io.Closeable
import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.IdGenerator
import popeye.proto.PackedPoints

trait PipelineSourceFactory {
  def startSource(sinkName: String, channel: PipelineChannel, config: Config, ect:ExecutionContext): Unit
}

trait PipelineSinkFactory {
  def startSink(sinkName: String, channel: PipelineChannel, config: Config, storagesConfig: Config, ect: ExecutionContext): Unit
}

trait PipelineChannelFactory {
  def make(actorSystem: ActorSystem, metrics: MetricRegistry, ect:ExecutionContext): PipelineChannel
}

trait PipelineChannel {
  def actorSystem: ActorSystem
  def metrics: MetricRegistry
  def idGenerator: IdGenerator
  def newWriter(): PipelineChannelWriter
  def startReader(group: String, sink: PointsSink): Unit
}

trait PipelineChannelWriter {
  def write(promise: Option[Promise[Long]], points: PackedPoints): Unit
}
