package popeye.storage.hbase

import akka.actor.{Props, ActorSystem, ActorRef}
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import popeye.util.hbase.HBaseConfigured
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration


class PointsStorageConfig(val config: Config,
                          val actorSystem: ActorSystem,
                          val metricRegistry: MetricRegistry,
                          val zkQuorum: String)
                          (implicit val eCtx: ExecutionContext){
  val uidsTableName = config.getString("table.uids")
  val pointsTableName = config.getString("table.points")
  val poolSize = config.getInt("pool.max")
  val resolveTimeout = new FiniteDuration(config.getMilliseconds(s"uids.resolve-timeout"), TimeUnit.MILLISECONDS)
  val uidsConfig = config.getConfig("uids")
}

/**
 * Encapsulates configured hbase client and points storage actors.
 * @param config provides necessary configuration parameters
 */
class PointsStorageConfigured(config: PointsStorageConfig) {

  val hbase = new HBaseConfigured(
    config.config, config.zkQuorum, config.poolSize)

  config.actorSystem.registerOnTermination(hbase.close())

  val storage = {
    implicit val eCtx = config.eCtx

    val uniqueIdStorage = new UniqueIdStorage(config.uidsTableName, hbase.hTablePool, HBaseStorage.UniqueIdMapping)
    val uniqIdResolved = config.actorSystem.actorOf(Props.apply(new UniqueIdActor(uniqueIdStorage)))
    val metrics = makeUniqueIdCache(config.uidsConfig, HBaseStorage.MetricKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    val attrNames = makeUniqueIdCache(config.uidsConfig, HBaseStorage.AttrNameKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    val attrValues = makeUniqueIdCache(config.uidsConfig, HBaseStorage.AttrValueKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    new PointsStorage(
      config.pointsTableName, hbase.hTablePool, metrics, attrNames, attrValues,
      new PointsStorageMetrics(config.metricRegistry), config.resolveTimeout)
  }

  private def makeUniqueIdCache(config: Config, kind: String, resolver: ActorRef,
                                storage: UniqueIdStorage, resolveTimeout: FiniteDuration)
                               (implicit eCtx: ExecutionContext): UniqueId = {
    new UniqueIdImpl(storage.kindWidth(kind), kind, resolver,
      config.getInt(s"$kind.initial-capacity"),
      config.getInt(s"$kind.max-capacity"),
      resolveTimeout
    )
  }

}
