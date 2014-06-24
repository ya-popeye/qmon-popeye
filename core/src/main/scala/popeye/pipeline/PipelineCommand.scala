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
import popeye.proto.Message.Point
import scala.collection.JavaConverters._

object PipelineCommand {

  val channels: Map[String, PipelineChannelFactory] = Map(
    "kafka" -> KafkaPipelineChannel.factory(),
    "memory" -> MemoryPipelineChannel.factory()
  )

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
                    idGenerator: IdGenerator,
                    shardAttributes: Set[String]): Map[String, PipelineSinkFactory] = {
    val kafkaStarter = new KafkaSinkStarter(actorSystem, ectx, idGenerator, metrics)
    val bulkloadStarter = new BulkloadSinkStarter(kafkaStarter, actorSystem.scheduler, ectx, metrics)
    val sinks = Map(
      "hbase-sink" -> new HBasePipelineSinkFactory(storagesConfig, actorSystem, ectx, shardAttributes, metrics),
      "kafka-sink" -> new KafkaSinkFactory(kafkaStarter),
      "bulkload-sink" -> new BulkloadSinkFactory(bulkloadStarter, storagesConfig),
      "blackhole" -> new BlackHolePipelineSinkFactory(actorSystem, ectx),
      "fail" -> new PipelineSinkFactory {
        def startSink(sinkName: String, config: Config): PointsSink = new PointsSink {
          override def sendPoints(batchId: Long, points: Point*): Future[Long] = {
            Future.failed(new RuntimeException("fail sink"))
          }

          override def sendPacked(batchId: Long, buffers: PackedPoints*): Future[Long] = {
            Future.failed(new RuntimeException("fail sink"))
          }

          override def close(): Unit = {}
        }
      }
    )                                            
    sinks.withDefault{
      typeName => throw new IllegalArgumentException(f"No such sink type: $typeName, available types: ${sinks.keys}")
    }
  }

  def sourceFactories(shardAttributes: Set[String]): Map[String, PipelineSourceFactory] = {
    val factories = Map(
      "telnet" -> TelnetPointsServer.sourceFactory(shardAttributes)
    )
    factories.withDefault(
      typeName =>
        throw new IllegalArgumentException(f"No such source type: $typeName, " +
          f"available types: ${ factories.keys }")
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

    val shardAttributeNames = config.getStringList("popeye.shard-attributes").asScala.toSet
    val storageConfig = config.getConfig("popeye.storages")
    val sinkFactories = PipelineCommand.sinkFactories(
      ectx,
      actorSystem,
      storageConfig,
      metrics,
      idGenerator,
      shardAttributeNames
    )
    val sourceFactories = PipelineCommand.sourceFactories(shardAttributeNames)
    val readersConfig = ConfigUtil.asMap(pc.getConfig("channelReaders"))

    for ((readerName, readerConfig) <- readersConfig) {
      val mainSinkConfig = readerConfig.getConfig("mainSink")
      val mainSink = sinkFactories(mainSinkConfig.getString("type")).startSink(f"$readerName-mainSink", mainSinkConfig)

      val dropSinkConfig = readerConfig.getConfig("dropSink")
      val dropSink = sinkFactories(dropSinkConfig.getString("type")).startSink(f"$readerName-dropSink", dropSinkConfig)

      channel.startReader("popeye-" + readerName, mainSink, dropSink)
    }
    for ((sourceName, sourceConfig) <- ConfigUtil.asMap(pc.getConfig("sources"))) {
      val typeName = sourceConfig.getString("type")
      sourceFactories(typeName).startSource(sourceName, channel, sourceConfig, ectx)
    }

    val monitoringAddress = config.getString("monitoring.address")
    val Array(host, port) = monitoringAddress.split(":")
    val address = new InetSocketAddress(host, port.toInt)
    MonitoringHttpServer.runServer(address, metrics, actorSystem)
  }
}
