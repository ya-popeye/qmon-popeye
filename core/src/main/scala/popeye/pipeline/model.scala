package popeye.pipeline
import scala.concurrent.Future
import java.io.Closeable
import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config

trait PipelineLifecycle {
  def start()
  def stop()
}

trait PipelineSourceFactory {
  def newSource(sinkName: String, channel: PipelineChannel, config: Config): PipelineSource
}

trait PipelineSinkFactory {
  def newSink(sinkName: String, channel: PipelineChannel, config: Config): PipelineSink
}

trait PipelineChannelFactory {
  def make(actorSystem: ActorSystem, metrics: MetricRegistry): PipelineChannel
}

trait PipelineChannel {
  def actorSystem: ActorSystem
  def metrics: MetricRegistry
  def newWriter(): PipelineChannelWriter
  def newReader(group: String, sink: PointsSink): PipelineChannelReader
}

trait PipelineSource extends PipelineLifecycle {
}

trait PipelineSink extends PipelineLifecycle {
}

trait PipelineChannelWriter {
  def write(): Future[Long]
}

trait PipelineChannelReader extends Closeable {
}

