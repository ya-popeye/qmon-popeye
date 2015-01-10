package popeye.storage.hbase

import com.typesafe.config.Config
import popeye.Logging
import popeye.pipeline._
import scala.concurrent.ExecutionContext
import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry

class HBasePipelineSinkFactory(storagesConfig: Config,
                               actorSystem: ActorSystem,
                               ectx: ExecutionContext,
                               shardAttributes: Set[String],
                               metrics: MetricRegistry)
  extends PipelineSinkFactory with Logging {
  def startSink(sinkName: String, config: Config): PointsSink = {
    info("starting sink...")
    val storageName = config.getString("storage")
    val storageConfig: HBaseStorageConfig = HBaseStorageConfig(
      config.withFallback(storagesConfig.getConfig(storageName)),
      shardAttributes,
      sinkName
    )
    val hbaseStorage = new HBaseStorageConfigured(storageConfig, actorSystem, metrics)(ectx)
    info("checking hbase...")
    hbaseStorage.storage.ping()
    info("sink is ready")
    new HBasePointsSink(hbaseStorage.storage)(ectx)
  }
}
