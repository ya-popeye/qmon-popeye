package popeye.transport

import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTablePool
import popeye.storage.hbase._
import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration


/**
 * @author Andrey Stepachev
 */
object PumpMain extends PopeyeMain("pump") {
  val hbaseConfig = makeHBaseConfig(config.getConfig("hbase.client"))
  hbaseConfig.set("hbase.zookeeper.quorum", config.getString("zk.cluster"))
  val hTablePool = new HTablePool(hbaseConfig, config.getInt("hbase.pool.max"))

  import scala.concurrent.ExecutionContext.Implicits.global

  system.registerOnTermination(hTablePool.close())

  val resolveTimeout = new FiniteDuration(config.getMilliseconds(s"hbase.uids.resolve-timeout"), TimeUnit.MILLISECONDS)

  val uniqueIdStorage = new UniqueIdStorage(config.getString("hbase.table.uids"), hTablePool, HBaseStorage.UniqueIdMapping)
  val uniqIdResolved = system.actorOf(Props.apply(new UniqueIdActor(uniqueIdStorage)))
  val uidsConfig = config.getConfig("hbase.uids")
  val metrics = makeUniqueIdCache(uidsConfig, HBaseStorage.MetricKind, uniqIdResolved, uniqueIdStorage, resolveTimeout)
  val attrNames = makeUniqueIdCache(uidsConfig, HBaseStorage.AttrNameKind, uniqIdResolved, uniqueIdStorage, resolveTimeout)
  val attrValues = makeUniqueIdCache(uidsConfig, HBaseStorage.AttrValueKind, uniqIdResolved, uniqueIdStorage, resolveTimeout)
  val storage = new PointsStorage(config.getString("hbase.table.points"), hTablePool, metrics, attrNames, attrValues, resolveTimeout)
  val consumer = HBasePointConsumer.start(config, storage)

  private def makeUniqueIdCache(config: Config, kind: String, resolver: ActorRef, storage: UniqueIdStorage, resolveTimeout: FiniteDuration): UniqueId = {
    new UniqueId(storage.kindWidth(kind), kind, resolver,
      config.getInt(s"$kind.initial-capacity"),
      config.getInt(s"$kind.max-capacity"),
      resolveTimeout
    )
  }

  private def makeHBaseConfig(config: Config): Configuration = {
    val hbaseConfig = HBaseConfiguration.create
    config.entrySet() foreach { entry =>
      hbaseConfig.set(entry.getKey, entry.getValue.unwrapped().toString())
    }
    hbaseConfig
  }
}
