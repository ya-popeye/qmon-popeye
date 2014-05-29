package popeye.pipeline

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.pipeline.kafka.{KafkaSinkStarter, KafkaSinkFactory, KafkaPipelineChannel}
import popeye.storage.hbase.HBasePipelineSinkFactory
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import popeye.storage.BlackHolePipelineSinkFactory
import popeye.pipeline.server.telnet.TelnetPointsServer
import popeye.pipeline.kafka.KafkaPipelineChannel
import popeye.pipeline.memory.MemoryPipelineChannel
import popeye.proto.PackedPoints
import popeye.monitoring.MonitoringHttpServer
import java.net.InetSocketAddress
import scopt.OptionParser
import popeye.{ConfigUtil, PopeyeCommand, IdGenerator, MainConfig}
import popeye.pipeline.sinks.{BulkloadSinkStarter, BulkloadSinkFactory}
import akka.dispatch.ExecutionContexts
import java.util.concurrent.Executors
import com.google.common.util.concurrent.ThreadFactoryBuilder

object PipelineCommand {

  val channels: Map[String, PipelineChannelFactory] = Map(
    "kafka" -> KafkaPipelineChannel.factory(),
    "memory" -> MemoryPipelineChannel.factory()
  )

  val sources: Map[String, PipelineSourceFactory] = Map(
    "telnet" -> TelnetPointsServer.sourceFactory())

  def channelsForType(channelType: String): PipelineChannelFactory = {
    channels.getOrElse(
      channelType,
      throw new IllegalArgumentException(f"No such channel type: $channelType, " +
        f"available types: ${channels.keys}")
    )
  }

  def sinkFactories(ectx: ExecutionContext,
                    actorSystem: ActorSystem,
                    storagesConfig: Config,
                    metrics: MetricRegistry,
                    idGenerator: IdGenerator): Map[String, PipelineSinkFactory] = {
    val kafkaStarter = new KafkaSinkStarter(actorSystem, ectx, idGenerator, metrics)
    val bulkloadStarter = new BulkloadSinkStarter(kafkaStarter, actorSystem.scheduler, ectx, metrics)
    val sinks = Map(
      "hbase-sink" -> new HBasePipelineSinkFactory(storagesConfig, actorSystem, ectx, metrics),
      "kafka-sink" -> new KafkaSinkFactory(kafkaStarter),
      "bulkload-sink" -> new BulkloadSinkFactory(bulkloadStarter, storagesConfig),
      "blackhole" -> new BlackHolePipelineSinkFactory(actorSystem, ectx),
      "fail" -> new PipelineSinkFactory {
        def startSink(sinkName: String, config: Config): PointsSink = new PointsSink {
          def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] =
            Future.failed(new RuntimeException("fail sink"))
        }
      }
    )                                            
    sinks.withDefault{
      typeName => throw new IllegalArgumentException(f"No such sink type: $typeName, available types: ${sinks.keys}")
    }
  }

  def sourceForType(typeName: String): PipelineSourceFactory = {
    sources.getOrElse(
      typeName,
      throw new IllegalArgumentException(f"No such source type: $typeName, " +
        f"available types: ${sources.keys}")
    )
  }
}

class PipelineCommand extends PopeyeCommand {
  def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "pipeline" action { (_, c) => c.copy(command = Some(this))}
    parser
  }

  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit = {
    val daemonThreadFatory = new ThreadFactoryBuilder().setDaemon(true).build()
    val ectx = ExecutionContexts.fromExecutorService(Executors.newCachedThreadPool(daemonThreadFatory))
    val pc = config.getConfig("popeye.pipeline")
    val idGenerator = new IdGenerator(
      config.getInt("generator.id"),
      config.getInt("generator.datacenter"))
    val context = new PipelineContext(actorSystem, metrics, idGenerator, ectx)

    val channelConfig = pc.getConfig("channel")
    val channelType: String = pc.getString("channel.type")
    val typeConfig: Config = channelConfig.getConfig(channelType)
    val channel = PipelineCommand.channelsForType(channelType)
      .make(typeConfig, context)

    val storageConfig = config.getConfig("popeye.storages")
    val sinks = PipelineCommand.sinkFactories(ectx, actorSystem, storageConfig, metrics, idGenerator)
    val readersConfig = ConfigUtil.asMap(pc.getConfig("channelReaders"))

    for ((readerName, readerConfig) <- readersConfig) {
      val mainSinkConfig = readerConfig.getConfig("mainSink")
      val mainSink = sinks(mainSinkConfig.getString("type")).startSink(f"$readerName-mainSink", mainSinkConfig)

      val dropSinkConfig = readerConfig.getConfig("dropSink")
      val dropSink = sinks(dropSinkConfig.getString("type")).startSink(f"$readerName-dropSink", dropSinkConfig)

      channel.startReader("popeye-" + readerName, mainSink, dropSink)
    }
    for ((sourceName, sourceConfig) <- ConfigUtil.asMap(pc.getConfig("sources"))) {
      val typeName = sourceConfig.getString("type")
      PipelineCommand.sourceForType(typeName).startSource(sourceName, channel, sourceConfig, ectx)
    }

    val monitoringAddress = config.getString("monitoring.address")
    val Array(host, port) = monitoringAddress.split(":")
    val address = new InetSocketAddress(host, port.toInt)
    MonitoringHttpServer.runServer(address, metrics, actorSystem)
  }
}
