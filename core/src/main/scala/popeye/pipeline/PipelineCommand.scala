package popeye.pipeline

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.pipeline.kafka.KafkaPipelineChannel
import popeye.storage.hbase.HBaseStorage
import popeye.{ConfigUtil, IdGenerator, MainConfig, PopeyeCommand}
import scala.concurrent.ExecutionContext
import scopt.OptionParser
import popeye.storage.BlackHole
import popeye.pipeline.server.telnet.TelnetPointsServer
import popeye.pipeline.memory.MemoryPipelineChannel

object PipelineCommand {

  val sources: Map[String, PipelineSourceFactory] = Map(
    "telnet" -> TelnetPointsServer.sourceFactory())

  val sinks: Map[String, PipelineSinkFactory] = Map(
    "hbase-sink" -> HBaseStorage.sinkFactory(),
    "blackhole" -> BlackHole.sinkFactory()
  )

  def sinkForType(typeName: String): PipelineSinkFactory = {
    sinks.getOrElse(
      typeName,
      throw new IllegalArgumentException(f"No such sink type: $typeName, available types: ${sinks.keys}")
    )
  }

  def sourceForType(typeName: String): PipelineSourceFactory = {
    sources.getOrElse(
      typeName,
      throw new IllegalArgumentException(f"No such source type: $typeName, available types: ${sources.keys}")
    )
  }
}

class PipelineCommand extends PopeyeCommand {
  def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "pipeline" action { (_, c) => c.copy(command = Some(this))}
    parser
  }

  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit = {

    val ectx = ExecutionContext.global
    val pc = config.getConfig("popeye.pipeline")
    val channelConfig = pc.getConfig("channel")
    val storageConfig = config.getConfig("popeye.storages")
    val idGenerator = new IdGenerator(config.getInt("generator.id"), config.getInt("generator.datacenter"))
    val channel = pc.getString("channel.type") match {
      case "kafka" =>
        new KafkaPipelineChannel(
          channelConfig.getConfig("kafka"),
          actorSystem, ectx, metrics, idGenerator)
      case "memory" =>
        new MemoryPipelineChannel(
          channelConfig.getConfig("memory"),
          actorSystem, metrics, idGenerator)
      case x =>
        throw new NoSuchElementException(s"Requested channel type not supported")

    }

    for ((sinkName, sinkConfig) <- ConfigUtil.asMap(pc.getConfig("sinks"))) {
      val typeName = sinkConfig.getString("type")
      PipelineCommand.sinkForType(typeName).startSink(sinkName, channel, sinkConfig, storageConfig, ectx)
    }
    for ((sourceName, sourceConfig) <- ConfigUtil.asMap(pc.getConfig("sources"))) {
      val typeName = sourceConfig.getString("type")
      PipelineCommand.sourceForType(typeName).startSource(sourceName, channel, sourceConfig, ectx)
    }
  }

}
