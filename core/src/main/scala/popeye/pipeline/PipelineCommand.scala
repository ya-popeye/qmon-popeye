package popeye.pipeline

import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.storage.hbase.HBaseStorage
import popeye.transport.server.http.HttpPointsServer
import popeye.{MainConfig, PopeyeCommand}
import scala.collection.JavaConversions._
import scopt.OptionParser
import akka.actor.ActorSystem

object PipelineCommand {

  val channels: Map[String, PipelineChannelFactory] = Map("kafka" -> KafkaPipelineChannel.factory())
  val sources: Map[String, PipelineSourceFactory] = Map("http" -> HttpPointsServer.sourceFactory())
  val sinks: Map[String, PipelineSinkFactory] = Map("hbase" -> HBaseStorage.sinkFactory())

  def sinkForType(typeName: String): PipelineSinkFactory = {
    sinks.getOrElse(typeName, throw new IllegalArgumentException("No such sink type"))
  }

  def sourceForType(typeName: String): PipelineSourceFactory = {
    sources.getOrElse(typeName, throw new IllegalArgumentException("No such source type"))
  }
}


/**
 * @author Andrey Stepachev
 */
class PipelineCommand extends PopeyeCommand {
  def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "pipeline" action { (_, c) => c}
    parser
  }

  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit = {

    val pc = config.getConfig("popeye.pipeline")
    val channel = PipelineCommand.channels.getOrElse(pc.getString("channel"),
      throw new NoSuchElementException(s"Requested channel type not supported"))
      .make(actorSystem, metrics)

    pc.getStringList("popeye.sinks").map { sn =>
      val sinkConfig = pc.getConfig(s"popeye.$sn").withFallback(pc)
      PipelineCommand.sinkForType(sn).newSink(sn, channel, sinkConfig)
    }.foreach(_.start())

    pc.getStringList("popeye.sources").map { sn =>
      val sinkConfig = pc.getConfig(s"popeye.$sn").withFallback(pc)
      PipelineCommand.sourceForType(sn).newSource(sn, channel, sinkConfig)
    }.foreach(_.start())
  }
}
