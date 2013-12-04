package popeye.storage.hbase

import com.typesafe.config.Config
import popeye.pipeline._
import scala.concurrent.ExecutionContext

class HBasePipelineSinkFactory extends PipelineSinkFactory {
  def startSink(sinkName: String, channel: PipelineChannel, config: Config, storagesConfig: Config, ectx: ExecutionContext): Unit = {
    val storageName = config.getString("storage")
    val hbaseStorage = new HBaseStorageConfigured(
      new HBaseStorageConfig(
        config.withFallback(storagesConfig.getConfig(storageName)),
        channel.actorSystem,
        channel.metrics,
        sinkName
      )(ectx))
    hbaseStorage.storage.ping()
    channel.startReader(
      "popeye-" + sinkName,
      new HBasePointsSink(config, hbaseStorage.storage)(ectx))
  }
}
