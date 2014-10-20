package popeye.query

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.storage.hbase.{HBaseStorageConfig, HBaseStorageConfigured}
import popeye._
import scala.concurrent.ExecutionContext
import scopt.OptionParser
import popeye.MainConfig
import scala.collection.JavaConverters._

object QueryCommand extends PopeyeCommand with Logging {
  val serverTypes: Map[String, HttpServerFactory] = Map(
    "opentsdb" -> OpenTSDB2HttpApiServer,
    "simple" -> HttpQueryServer,
    "health-check" -> HealthCheckServer
  ).withDefault {
    key =>
      throw new NoSuchElementException(f"wrong server type name: $key; available types: ${serverTypes.keys} ")
  }

  def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "query" action {
      (_, c) => c.copy(command = Some(this))
    }
    parser
  }

  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config): Unit = {
    val shardAttributeNames = config.getStringList("popeye.shard-attributes").asScala.toSet
    val queryConfig = config.getConfig("popeye.query")
    val storagesConfig = config.getConfig("popeye.storages")
    val storageName = queryConfig.getString("db.storage")
    val hbaseConfig = queryConfig.getConfig("db").withFallback(storagesConfig.getConfig(storageName))
    val serverConfig = queryConfig.getConfig("server")
    val serverTypeKey = serverConfig.getString("type")
    val ectx = ExecutionContext.global
    val storageConfig = new HBaseStorageConfig(hbaseConfig, shardAttributeNames)
    val hbaseStorage = new HBaseStorageConfigured(storageConfig, actorSystem, metrics)(ectx)
    hbaseStorage.storage.ping()
    val pointsStorage = PointsStorage.createPointsStorage(
      hbaseStorage.storage,
      hbaseStorage.uniqueIdStorage,
      storageConfig.timeRangeIdMapping,
      ectx
    )
    serverTypes(serverTypeKey).runServer(serverConfig, pointsStorage, actorSystem, ectx)
  }
}
