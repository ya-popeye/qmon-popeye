package popeye.query

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.storage.hbase.{HBaseStorageConfig, HBaseStorageConfigured}
import popeye._
import scala.concurrent.ExecutionContext
import scopt.OptionParser
import popeye.MainConfig
import scala.Some

object QueryCommand {
}

class QueryCommand extends PopeyeCommand with Logging {
  def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "query" action {
      (_, c) => c.copy(command = Some(this))
    }
    parser
  }

  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit = {
    val popeyeConfig = config.getConfig("popeye.pipeline")
    val hbaseConfig = ConfigUtil.mergeDefaults(popeyeConfig, "hbase", "query.db")
    val serverConfig = popeyeConfig.getConfig("query.http")
    val ectx = ExecutionContext.global
    val hbaseStorage = new HBaseStorageConfigured(
      new HBaseStorageConfig(
        hbaseConfig,
        actorSystem,
        metrics
      )(ectx))
    hbaseStorage.storage.ping()

    HttpQueryServer.runServer(serverConfig, hbaseStorage.storage)(actorSystem)
  }
}
