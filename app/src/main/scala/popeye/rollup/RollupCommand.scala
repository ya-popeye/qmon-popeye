package popeye.rollup

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import popeye.rollup.RollupMapperEngine.RollupStrategy
import popeye.storage.hbase.HBaseStorageConfig
import popeye.Logging
import popeye.util.hbase.HBaseConfigured
import popeye.{MainConfig, PopeyeCommand}
import scopt.OptionParser
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.{Path, FileSystem}

object RollupCommand extends PopeyeCommand with Logging {

  case class RollupArgs(startTime: Int, stopTime: Int, strategy: RollupStrategy)

  val defaultRollupArgs = RollupArgs(-1, -1, RollupStrategy.HourRollup)

  private def updateRollupArgs(mainConfig: MainConfig)(f: RollupArgs => RollupArgs): MainConfig = {
    val updatedArgs = mainConfig.commandArgs.map {
      args => f(args.asInstanceOf[RollupArgs])
    }.getOrElse {
      f(defaultRollupArgs)
    }
    mainConfig.copy(commandArgs = Some(updatedArgs))
  }

  override def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "rollup" action { (_, c) => c.copy(command = Some(this))}
    parser.opt[Int]("start_time").action {
      (param, config) => updateRollupArgs(config) {
        args => args.copy(startTime = param)
      }
    }
    parser.opt[Int]("stop_time").action {
      (param, config) => updateRollupArgs(config) {
        args => args.copy(stopTime = param)
      }
    }
    parser.opt[String]("granularity").action {
      (param, config) => updateRollupArgs(config) {
        args =>
          args.copy(strategy = RollupStrategy.parseString(param))
      }
    }
    parser
  }

  override def run(actorSystem: ActorSystem,
                   metrics: MetricRegistry,
                   config: Config,
                   commandArgs: Option[Any]): Unit = {
    info("starting....")
    val RollupArgs(startTime, stopTime, strategy) = commandArgs.get
    checkRangeBoundary(strategy, startTime)
    checkRangeBoundary(strategy, stopTime)
    val shardAttributeNames = config.getStringList("popeye.shard-attributes").asScala.toSet
    val rollupConfig = config.getConfig("popeye.rollup")
    val restoreDir = rollupConfig.getString("restoreDir")
    val outputDir = rollupConfig.getString("outputDir")
    val jarsDir = rollupConfig.getString("jarsDir")
    val storagesConfig = config.getConfig("popeye.storages")
    val storageName = rollupConfig.getString("db.storage")
    //    val lockConfig = rollupConfig.getConfig("lock")
    //    val timeout = rollupConfig.getMilliseconds("timeout").toInt.millis
    //    val lockZkConnect = ZkConnect.parseString(lockConfig.getString("zk.quorum"))
    //    val lockPath = lockConfig.getString("path")

    val hBaseConfig = rollupConfig.getConfig("db").withFallback(storagesConfig.getConfig(storageName))
    val storageConfig = HBaseStorageConfig(hBaseConfig, shardAttributeNames)

    val hBase = new HBaseConfigured(storageConfig.hbaseConfig, storageConfig.zkQuorum)

    val Seq(restoreQDir, outputQDir) = {
      val paths = Seq(restoreDir, outputDir).map(p => new Path(p))
      makePathsQualified(paths, storageConfig.hadoopConfiguration)
    }

    val jarPaths = getSubPaths(new Path(jarsDir), storageConfig.hadoopConfiguration)
    val rollupRunner = new RollupJobRunner(
      hBaseConfiguration = hBase.hbaseConfiguration,
      pointsTableName = TableName.valueOf(storageConfig.pointsTableName),
      hadoopConfiguration = storageConfig.hadoopConfiguration,
      restoreDirParent = restoreQDir,
      outputPathParent = outputQDir,
      jarPaths = jarPaths,
      tsdbFormatConfig = storageConfig.tsdbFormatConfig
    )

    val generationIdMapping = storageConfig.tsdbFormatConfig.generationIdMapping
    val generations = generationIdMapping.backwardIterator(stopTime).takeWhile(_.stop > startTime)
    for (generation <- generations) {
      val genStartTime = math.max(generation.start, startTime)
      val genStopTime = math.min(generation.stop, stopTime)
      rollupRunner.doRollup(generation.id, strategy, genStartTime, genStopTime)
    }
    actorSystem.shutdown()
  }

  def makePathsQualified(paths: Seq[Path], hadoopConfiguration: Configuration) = {
    val hdfs = FileSystem.newInstance(hadoopConfiguration)
    try {
      paths.map(hdfs.makeQualified)
    } finally {
      hdfs.close()
    }
  }

  def getSubPaths(path: Path, hadoopConfiguration: Configuration) = {
    val hdfs = FileSystem.newInstance(hadoopConfiguration)
    try {
      hdfs.listStatus(path).toSeq.map(_.getPath)
    } finally {
      hdfs.close()
    }
  }

  def checkRangeBoundary(rollupStrategy: RollupStrategy, baseStartTime: Int) {
    val resolutionInSeconds = rollupStrategy.maxResolutionInSeconds
    require(
      baseStartTime % resolutionInSeconds == 0,
      f"$rollupStrategy do not accept $baseStartTime as a range boundary; " +
        f"max resolution in seconds = $resolutionInSeconds; " +
        f"$baseStartTime ${"%"} $resolutionInSeconds = ${baseStartTime % resolutionInSeconds}, not 0"
    )
  }
}
