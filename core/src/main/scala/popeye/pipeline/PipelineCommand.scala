package popeye.pipeline

import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.storage.hbase.HBaseStorage
import popeye.{IdGenerator, MainConfig, PopeyeCommand}
import scala.collection.JavaConversions._
import scopt.OptionParser
import akka.actor.ActorSystem
import popeye.pipeline.kafka.KafkaPipelineChannel
import scala.concurrent.ExecutionContext

object PipelineCommand {

  val sources: Map[String, PipelineSourceFactory] = Map()//"http" -> HttpPointsServer.sourceFactory())
  val sinks: Map[String, PipelineSinkFactory] = Map("hbase" -> HBaseStorage.sinkFactory())

  def sinkForType(typeName: String): PipelineSinkFactory = {
    sinks.getOrElse(typeName, throw new IllegalArgumentException("No such sink type"))
  }

  def sourceForType(typeName: String): PipelineSourceFactory = {
    sources.getOrElse(typeName, throw new IllegalArgumentException("No such source type"))
  }
}

class PipelineCommand extends PopeyeCommand {
  def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "pipeline" action { (_, c) => c}
    parser
  }

  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit = {

    val pc = config.getConfig("popeye.pipeline")
    val channel = pc.getString("channel") match {
      case "kafka" =>
        new KafkaPipelineChannel(config, actorSystem, metrics,
          new IdGenerator(config.getInt("generator.id"), config.getInt("generator.datacenter")))
      case x =>
        throw new NoSuchElementException(s"Requested channel type not supported")

    }

    val ectx = ExecutionContext.global

    pc.getStringList("popeye.sinks").foreach { sn =>
      val sinkConfig = pc.getConfig(s"popeye.$sn").withFallback(pc)
      PipelineCommand.sinkForType(sn).startSink(sn, channel, sinkConfig, ectx)
    }

    pc.getStringList("popeye.sources").foreach { sn =>
      val sinkConfig = pc.getConfig(s"popeye.$sn").withFallback(pc)
      PipelineCommand.sourceForType(sn).startSource(sn, channel, sinkConfig, ectx)
    }
  }
}
