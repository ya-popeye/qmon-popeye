package popeye.pipeline

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.pipeline.kafka.KafkaPipelineChannel
import popeye.storage.hbase.HBaseStorage
import popeye.{ConfigUtil, IdGenerator, MainConfig, PopeyeCommand}
import scala.concurrent.ExecutionContext
import scopt.OptionParser

object PipelineCommand {

  val sources: Map[String, PipelineSourceFactory] = Map()
  //"http" -> HttpPointsServer.sourceFactory())
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
    parser cmd "pipeline" action { (_, c) => c.copy(command = Some(this))}
    parser
  }

  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit = {

    val pc = config.getConfig("popeye.pipeline")
    val idGenerator = new IdGenerator(config.getInt("generator.id"), config.getInt("generator.datacenter"))
    val channel = pc.getString("channel.type") match {
      case "kafka" =>
        new KafkaPipelineChannel(
          ConfigUtil.mergeDefaults(pc, "kafka", "channel.kafka"),
          actorSystem, metrics, idGenerator)
      case x =>
        throw new NoSuchElementException(s"Requested channel type not supported")

    }

    val ectx = ExecutionContext.global

    ConfigUtil.foreachKeyValue(pc, "sinks") { (typeName, confName) =>
      val sinkConfig = ConfigUtil.mergeDefaults(pc, typeName, confName)
      PipelineCommand.sinkForType(typeName).startSink(confName, channel, sinkConfig, ectx)
    }

    ConfigUtil.foreachKeyValue(pc, "sources") { (typeName, confName) =>
      val sourceConfig = ConfigUtil.mergeDefaults(pc, typeName, confName)
      PipelineCommand.sourceForType(typeName).startSource(confName, channel, sourceConfig, ectx)
    }
  }

}
