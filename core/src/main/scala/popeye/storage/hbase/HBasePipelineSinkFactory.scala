package popeye.storage.hbase

import com.typesafe.config.Config
import popeye.pipeline._
import scala.concurrent.ExecutionContext
import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry

class HBasePipelineSinkFactory(storagesConfig: Config,
                               actorSystem: ActorSystem,
                               ectx: ExecutionContext,
                               metrics: MetricRegistry)
  extends PipelineSinkFactory {
  def startSink(sinkName: String, config: Config): PointsSink = {
    val storageName = config.getString("storage")
    val hbaseStorage = new HBaseStorageConfigured(
      new HBaseStorageConfig(
        config.withFallback(storagesConfig.getConfig(storageName)),
        actorSystem,
        metrics,
        sinkName
      )(ectx))
    hbaseStorage.storage.ping()

    new HBasePointsSink(hbaseStorage.storage)(ectx)
  }
}
