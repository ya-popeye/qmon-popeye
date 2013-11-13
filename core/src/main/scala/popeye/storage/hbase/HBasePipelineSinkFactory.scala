package popeye.storage.hbase

import com.typesafe.config.Config
import popeye.pipeline._
import scala.concurrent.ExecutionContext

class HBasePipelineSinkFactory extends PipelineSinkFactory {
  def startSink(sinkName: String, channel: PipelineChannel, config: Config, ectx:ExecutionContext): Unit = {
    val hbaseStorage = new HBaseStorageConfigured(
      new HBaseStorageConfig(
        config,
        channel.actorSystem,
        channel.metrics,
        sinkName
      )(ectx))
    channel.startReader(
      "popeye-" + sinkName,
      new HBasePointsSink(config, hbaseStorage.storage)(ectx))
  }
}
