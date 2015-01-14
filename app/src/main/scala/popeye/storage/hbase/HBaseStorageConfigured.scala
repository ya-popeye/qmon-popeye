package popeye.storage.hbase

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Props, ActorSystem}
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.client.HTablePool
import popeye.Logging
import popeye.util.ZkConnect
import popeye.util.hbase.HBaseConfigured

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
 * Encapsulates configured hbase client and points storage actors.
 * @param config provides necessary configuration parameters
 */
class HBaseStorageConfigured(config: HBaseStorageConfig, actorSystem: ActorSystem, metricRegistry: MetricRegistry)
                            (implicit val eCtx: ExecutionContext) extends Logging {

  info(f"initializing HBaseStorage, config: $config")

  val hbase = new HBaseConfigured(config.hbaseConfig, config.zkQuorum)
  val hTablePool: HTablePool = hbase.getHTablePool(config.poolSize)

  TsdbTables.createTables(
    hbase.hbaseConfiguration,
    config.pointsTableName,
    config.uidsTableName,
    config.pointsTableCoprocessorJarPathOption
  )

  actorSystem.registerOnTermination(hTablePool.close())

  val uniqueIdStorage = {
    val metrics = new UniqueIdStorageMetrics("uniqueid.storage", metricRegistry)
    new UniqueIdStorage(config.uidsTableName, hTablePool, metrics)
  }

  val storage = {
    val uniqIdResolver = actorSystem.actorOf(Props.apply(UniqueIdActor(uniqueIdStorage, actorSystem.dispatcher)))
    val uniqueId = new UniqueIdImpl(
      uniqIdResolver,
      new UniqueIdMetrics("uniqueid", metricRegistry),
      config.uidsCacheInitialCapacity,
      config.uidsCacheMaxCapacity,
      config.resolveTimeout
    )
    val metrics: HBaseStorageMetrics = new HBaseStorageMetrics(config.storageName, metricRegistry)
    val tsdbFormat = config.tsdbFormatConfig.tsdbFormat
    new HBaseStorage(
      config.pointsTableName,
      hTablePool,
      uniqueId,
      tsdbFormat,
      metrics,
      config.resolveTimeout,
      config.readChunkSize)
  }
}


object HBaseStorageConfig {
  def apply(config: Config, shardAttributeNames: Set[String], storageName: String = "hbase"): HBaseStorageConfig = {
    import scala.collection.JavaConverters._
    val uidsTableName = config.getString("tables.uids.name")
    val pointsTableName = config.getString("tables.points.name")
    def hadoopConfiguration = {
      val conf = new Configuration()
      for (path <- config.getStringList("hadoop.conf.paths").asScala) {
        conf.addResource(new File(path).toURI.toURL)
      }
      conf
    }
    val pointsTableCoprocessorJarPathOption = {
      val coprocessorJarKey = "tables.points.coprocessor.jar.path"
      if (config.hasPath(coprocessorJarKey)) {
        val hdfs = FileSystem.newInstance(hadoopConfiguration)
        val jarPath = try {
          val pathString = config.getString(coprocessorJarKey)
          hdfs.makeQualified(new Path(pathString))
        }
        Some(jarPath)
      } else {
        None
      }
    }
    val poolSize = config.getInt("pool.max")
    val zkQuorum = ZkConnect.parseString(config.getString("zk.quorum"))
    val uidsConfig = config.getConfig("uids")
    val resolveTimeout = new FiniteDuration(uidsConfig.getMilliseconds(s"resolve-timeout"), TimeUnit.MILLISECONDS)
    val readChunkSize = config.getInt("read-chunk-size")
    val tsdbFormatConfig = {
      val startTimeAndPeriods = StartTimeAndPeriod.parseConfigList(config.getConfigList("generations"))
      TsdbFormatConfig(startTimeAndPeriods, shardAttributeNames)
    }
    val uidsCacheInitialCapacity = uidsConfig.getInt("cache.initial-capacity")
    val uidsCacheMaxCapacity = uidsConfig.getInt("cache.max-capacity")

    HBaseStorageConfig(
      config,
      hadoopConfiguration,
      uidsTableName,
      pointsTableName,
      pointsTableCoprocessorJarPathOption,
      poolSize,
      zkQuorum,
      readChunkSize,
      resolveTimeout,
      uidsCacheInitialCapacity,
      uidsCacheMaxCapacity,
      tsdbFormatConfig,
      storageName
    )
  }
}

case class HBaseStorageConfig(hbaseConfig: Config,
                              private val hadoopConfigurationInner: Configuration,
                              uidsTableName: String,
                              pointsTableName: String,
                              pointsTableCoprocessorJarPathOption: Option[Path],
                              poolSize: Int,
                              zkQuorum: ZkConnect,
                              readChunkSize: Int,
                              resolveTimeout: FiniteDuration,
                              uidsCacheInitialCapacity: Int,
                              uidsCacheMaxCapacity: Int,
                              tsdbFormatConfig: TsdbFormatConfig,
                              storageName: String) {
  def hadoopConfiguration = new Configuration(hadoopConfigurationInner)
}


