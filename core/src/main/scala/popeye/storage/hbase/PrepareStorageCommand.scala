package popeye.storage.hbase

import java.util
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.github.bigtoast.zookeeper.AsyncZooKeeperClient
import com.typesafe.config.Config
import org.apache.hadoop.hbase.{NotServingRegionException, TableName}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.zookeeper.CreateMode
import popeye.util.ZkConnect
import popeye.util.hbase.HBaseConfigured
import popeye.{Logging, MainConfig, PopeyeCommand}
import scopt.OptionParser
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.Try

object PrepareStorageCommand extends PopeyeCommand with Logging {
  override def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "prepare-storage" action { (_, c) => c.copy(command = Some(this)) }
    parser
  }

  override def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit = {
    info("starting....")
    val shardAttributeNames = config.getStringList("popeye.shard-attributes").asScala.toSet
    val prepareStorageConfig = config.getConfig("popeye.prepare-storage")
    val storagesConfig = config.getConfig("popeye.storages")
    val splits = prepareStorageConfig.getInt("db.splits")
    val storageName = prepareStorageConfig.getString("db.storage")
    val hBaseConfig = prepareStorageConfig.getConfig("db").withFallback(storagesConfig.getConfig(storageName))
    val lockConfig = prepareStorageConfig.getConfig("lock")
    val timeout = prepareStorageConfig.getMilliseconds("timeout").toInt.millis
    val retryInterval = prepareStorageConfig.getMilliseconds("retry-interval").toInt.millis
    val lockZkConnect = ZkConnect.parseString(lockConfig.getString("zk.quorum"))
    val lockPath = lockConfig.getString("path")
    val storageConfig = new HBaseStorageConfig(hBaseConfig, shardAttributeNames)
    val hBase = new HBaseConfigured(storageConfig.config, storageConfig.zkQuorum)
    val currentTimeInSeconds = (System.currentTimeMillis() / 1000).toInt
    val currentGenerationPeriod = storageConfig.timeRangeIdMapping.backwardIterator(currentTimeInSeconds).next()
    val nextGenerationId =
      storageConfig.timeRangeIdMapping.backwardIterator(currentGenerationPeriod.stop).next().id
    val pointsTableName = TableName.valueOf(storageConfig.pointsTableName)
    info("connecting to hbase...")
    val hBaseAdmin = new HBaseAdmin(hBase.hbaseConfiguration)
    info("connected to hbase")
    implicit val eCtx = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
    try {
      val zk = new AsyncZooKeeperClient(
        servers = lockZkConnect.toZkConnectString,
        sessionTimeout = 4000,
        connectTimeout = 4000,
        basePath = "/",
        eCtx
      )
      val utils = new PrepareCommandUtils(hBaseAdmin, pointsTableName, splits, retryInterval)
      try {
        val splitFuture = for {
          _ <- zk.createPath(lockPath)
          _ <- zk.create(s"$lockPath/lock", None, CreateMode.EPHEMERAL)
        } yield {
          info("acquired lock")
          val currGenSplits = utils.getNewSplits(currentGenerationPeriod.id)
          val nextGenSplits = utils.getNewSplits(nextGenerationId)
          val newSplits = currGenSplits ++ nextGenSplits
          for (split <- newSplits) {
            utils.doSplit(split)
          }
          if (newSplits.nonEmpty) {
            utils.doBalance()
          }
        }
        Await.result(splitFuture, timeout)
      } finally {
        zk.close
      }
    } finally {
      Try(hBaseAdmin.close())
      Try(actorSystem.shutdown())
      Try(eCtx.shutdown())
    }
  }
}

class PrepareCommandUtils(hBaseAdmin: HBaseAdmin,
                          pointsTableName: TableName,
                          nSplits: Int,
                          retryInterval: FiniteDuration) extends Logging {

  def getNewSplits(generationId: Short): Seq[Array[Byte]] = {
    val generationPrefix = Bytes.toBytes(generationId)
    info("getting table regions")
    val regions = hBaseAdmin.getTableRegions(pointsTableName).asScala
    info("got table regions")
    val generationRegions = regions.filter {
      regionInfo =>
        val regionGenerationPrefix = regionInfo.getStartKey.slice(0, 2)
        util.Arrays.equals(regionGenerationPrefix, generationPrefix)
    }
    val currentSplitPoints = generationRegions.map {
      regionInfo => new BytesKey(regionInfo.getStartKey)
    }.toSet
    info(s"current splits: ${ prettyPrintByteKeys(currentSplitPoints.toList) }")
    val splits = createSplits(generationPrefix, nSplits)
    val splitsSet = splits.map(array => new BytesKey(array)).toSet
    info(s"required splits: ${ prettyPrintByteKeys(splitsSet.toList) }")
    val isPreviousSplitOperationFailed =
      currentSplitPoints.forall(split => splitsSet.contains(split)) && currentSplitPoints.size != splitsSet.size
    if (isPreviousSplitOperationFailed || generationRegions.size < nSplits) {
      splits.filterNot {
        split => currentSplitPoints.contains(new BytesKey(split))
      }
    } else {
      Seq.empty
    }
  }

  def doSplit(split: Array[Byte]): Unit = {
    info(s"splitting on ${ Bytes.toStringBinary(split) }")
    doAndRetryIfRegionIsNotServing {
      hBaseAdmin.split(pointsTableName.getName, split)
    }
    info("waiting for region")
    waitForRegion(split)
    info("region is available")
  }

  def doBalance(retries: Int = 10): Unit = {
    var trials = 0
    hBaseAdmin.setBalancerRunning(true, true)
    while(true) {
      if (trials > retries) {
        return
      }
      info("calling balancer")
      val success = hBaseAdmin.balancer()
      trials += 1
      if (success) {
        info("balancer succeeded")
        return
      } else {
        warn("balancer failed")
      }
      Thread.sleep(retryInterval.toMillis.toInt)
    }
  }

  def prettyPrintByteKeys(byteKeys: List[BytesKey]) = {
    byteKeys.sorted.map(s => Bytes.toStringBinary(s.bytes)).mkString("\n", "\n", "\n")
  }

  def doAndRetryIfRegionIsNotServing(block: => Unit): Unit = {
    while(true) {
      try {
        block
        return
      } catch {
        case e: NotServingRegionException =>
          info("sleeping...")
          Thread.sleep(retryInterval.toMillis)
          info("retrying split")
      }
    }
  }

  def waitForRegion(startKey: Array[Byte]): Unit = {
    while(true) {
      try {
        val clusterStatus = hBaseAdmin.getClusterStatus
        val regionServers = clusterStatus.getServers.asScala
        val allRegions = regionServers.flatMap(server => hBaseAdmin.getOnlineRegions(server).asScala)
        if (allRegions.exists(region => Bytes.equals(region.getStartKey, startKey))) {
          return
        }
        info("sleeping...")
        Thread.sleep(retryInterval.toMillis)
      }
    }
  }

  def createSplits(prefix: Array[Byte], splits: Int): Seq[Array[Byte]] = {
    require(splits > 0, "number of splits should be greater than zero")
    val shortRange: Int = 0xffff // 65535
    val step: Double = if (splits > shortRange) 1 else shortRange.toDouble / splits
    for (i <- 1 until splits) yield {
      val splitInt: Int = (i * step).toInt
      val lastTwoBytes = Bytes.toBytes(splitInt).slice(2, 4)
      prefix ++ lastTwoBytes
    }
  }
}
