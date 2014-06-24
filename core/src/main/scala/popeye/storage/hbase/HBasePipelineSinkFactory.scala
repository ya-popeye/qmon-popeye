package popeye.storage.hbase

import com.typesafe.config.Config
import popeye.pipeline._
import scala.concurrent.ExecutionContext
import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry

class HBasePipelineSinkFactory(storagesConfig: Config,
                               actorSystem: ActorSystem,
                               ectx: ExecutionContext,
                               shardAttributes: Set[String],
                               metrics: MetricRegistry)
  extends PipelineSinkFactory {
  def startSink(sinkName: String, config: Config): PointsSink = {
    val storageName = config.getString("storage")
    val storageConfig: HBaseStorageConfig = new HBaseStorageConfig(
      config.withFallback(storagesConfig.getConfig(storageName)),
      shardAttributes,
      sinkName
    )
    val hbaseStorage = new HBaseStorageConfigured(storageConfig, actorSystem, metrics)(ectx)
    hbaseStorage.storage.ping()

    new HBasePointsSink(hbaseStorage.storage)(ectx)
  }
}
