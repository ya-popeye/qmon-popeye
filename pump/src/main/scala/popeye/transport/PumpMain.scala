package popeye.transport

import popeye.storage.opentsdb.TsdbPointConsumer
import popeye.storage.opentsdb.hbase._
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
  val hbaseConfig = makeHBaseConfig(config.getConfig("tsdb.hbase.client"))
  val hTablePool = new HTablePool(hbaseConfig, config.getInt("tsdb.hbase.pool.max"))

  import scala.concurrent.ExecutionContext.Implicits.global

  system.registerOnTermination(hTablePool.close())

  val resolveTimeout = new FiniteDuration(config.getMilliseconds(s"resolve-timeout"), TimeUnit.MILLISECONDS)

  val uniqueIdStorage = new UniqueIdStorage(config.getString("tsdb.hbase.uids-table"), hTablePool, HBaseStorage.UniqueIdMapping)
  val uniqIdResolved = system.actorOf(Props(classOf[UniqueIdActor], uniqueIdStorage))
  val metrics = makeUniqueIdCache(config.getConfig("tsdb.uid"), MetricsKind, uniqIdResolved, uniqueIdStorage, resolveTimeout)
  val attrNames = makeUniqueIdCache(config.getConfig("tsdb.uid"), AttrNameKind, uniqIdResolved, uniqueIdStorage, resolveTimeout)
  val attrValues = makeUniqueIdCache(config.getConfig("tsdb.uid"), AttrValueKind, uniqIdResolved, uniqueIdStorage, resolveTimeout)
  val storage = new PointsStorage(config.getString("tsdb.hbase.points-table"), hTablePool, metrics, attrNames, attrValues, resolveTimeout)
  val consumer = TsdbPointConsumer.start(config, storage)

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
