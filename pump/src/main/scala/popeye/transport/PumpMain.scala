package popeye.transport

import popeye.storage.hbase.HBasePointConsumer
import popeye.storage.hbase._
import org.apache.hadoop.hbase.client.HTablePool
import org.apache.hadoop.hbase.HBaseConfiguration
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._
import akka.actor.{ActorRef, Props}
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.util.concurrent.TimeUnit

import HBaseStorage._

/**
 * @author Andrey Stepachev
 */
object PumpMain extends PopeyeMain("pump") {
  val hbaseConfig = makeHBaseConfig(config.getConfig("hbase.client"))
  val hTablePool = new HTablePool(hbaseConfig, config.getInt("hbase.pool.max"))

  import scala.concurrent.ExecutionContext.Implicits.global

  system.registerOnTermination(hTablePool.close())

  val resolveTimeout = new FiniteDuration(config.getMilliseconds(s"resolve-timeout"), TimeUnit.MILLISECONDS)

  val uniqueIdStorage = new UniqueIdStorage(config.getString("hbase.uids-table"), hTablePool, HBaseStorage.UniqueIdMapping)
  val uniqIdResolved = system.actorOf(Props(classOf[UniqueIdActor], uniqueIdStorage))
  val uidsConfig = config.getConfig("hbase.uid")
  val metrics = makeUniqueIdCache(uidsConfig, MetricsKind, uniqIdResolved, uniqueIdStorage, resolveTimeout)
  val attrNames = makeUniqueIdCache(uidsConfig, AttrNameKind, uniqIdResolved, uniqueIdStorage, resolveTimeout)
  val attrValues = makeUniqueIdCache(uidsConfig, AttrValueKind, uniqIdResolved, uniqueIdStorage, resolveTimeout)
  val storage = new PointsStorage(config.getString("hbase.points-table"), hTablePool, metrics, attrNames, attrValues, resolveTimeout)
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
