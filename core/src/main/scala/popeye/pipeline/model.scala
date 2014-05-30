package popeye.pipeline
import scala.concurrent.{Promise, ExecutionContext}
import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.IdGenerator
import popeye.proto.PackedPoints

sealed class PipelineContext(val actorSystem: ActorSystem,
                             val metrics: MetricRegistry,
                             val idGenerator: IdGenerator,
                             val ectx: ExecutionContext)

trait PipelineSourceFactory {
  def startSource(sinkName: String, channel: PipelineChannel, config: Config, ect:ExecutionContext): Unit
}

trait PipelineSinkFactory {
  def startSink(sinkName: String, config: Config): PointsSink
}

trait PipelineChannelFactory {
  def make(config: Config, context: PipelineContext): PipelineChannel
}

trait PipelineChannel {
  def context(): PipelineContext
  def newWriter(): PipelineChannelWriter
  def startReader(group: String, mainSink: PointsSink, dropSink: PointsSink): Unit

  def actorSystem = context().actorSystem
  def metrics = context().metrics
  def idGenerator = context().idGenerator
  def ectx = context().ectx
}

trait PipelineChannelWriter {
  def write(promise: Option[Promise[Long]], points: PackedPoints): Unit
}
