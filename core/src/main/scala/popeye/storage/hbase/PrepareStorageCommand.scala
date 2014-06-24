package popeye.storage.hbase

import java.util

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import org.apache.hadoop.hbase.{NotServingRegionException, TableName}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import popeye.util.hbase.HBaseConfigured
import popeye.{Logging, MainConfig, PopeyeCommand}
import scopt.OptionParser
import scala.collection.JavaConverters._

object PrepareStorageCommand extends PopeyeCommand with Logging {
  override def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "prepare-storage" action { (_, c) => c.copy(command = Some(this)) }
    parser
  }

  override def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit = {
    val shardAttributeNames = config.getStringList("popeye.shard-attributes").asScala.toSet
    val prepareStorageConfig = config.getConfig("popeye.prepare-storage")
    val storagesConfig = config.getConfig("popeye.storages")
    val splits = prepareStorageConfig.getInt("db.splits")
    val storageName = prepareStorageConfig.getString("db.storage")
    val hBaseConfig = prepareStorageConfig.getConfig("db").withFallback(storagesConfig.getConfig(storageName))
    val storageConfig = new HBaseStorageConfig(hBaseConfig, shardAttributeNames)
    val hBase = new HBaseConfigured(storageConfig.config, storageConfig.zkQuorum)
    val hBaseAdmin = new HBaseAdmin(hBase.hbaseConfiguration)
    val currentTimeInSeconds = (System.currentTimeMillis() / 1000).toInt
    val currentGenerationPeriod = storageConfig.timeRangeIdMapping.backwardIterator(currentTimeInSeconds).next()
    val nextGenerationId =
      storageConfig.timeRangeIdMapping.backwardIterator(currentGenerationPeriod.stop).next().id
    val pointsTableName = TableName.valueOf(storageConfig.pointsTableName)
    try {
      splitIfNecessary(hBaseAdmin, pointsTableName, currentGenerationPeriod.id, splits)
      splitIfNecessary(hBaseAdmin, pointsTableName, nextGenerationId, splits)
    } finally {
      hBaseAdmin.close()
      actorSystem.shutdown()
    }
  }

  def splitIfNecessary(hBaseAdmin: HBaseAdmin,
                       pointsTableName: TableName,
                       generationId: Short,
                       nSplits: Int): Unit = {
    val generationPrefix = Bytes.toBytes(generationId)
    val regions = hBaseAdmin.getTableRegions(pointsTableName).asScala
    val generationRegions = regions.filter {
      regionInfo =>
        val regionGenerationPrefix = regionInfo.getStartKey.slice(0, 2)
        util.Arrays.equals(regionGenerationPrefix, generationPrefix)
    }
    val currentSplitPoints = generationRegions.map {
      regionInfo => new BytesKey(regionInfo.getStartKey)
    }.toSet
    val splits = createSplits(generationPrefix, nSplits)
    val splitsSet = splits.map(array => new BytesKey(array)).toSet
    val isPreviousSplitOperationFailed =
      currentSplitPoints.forall(split => splitsSet.contains(split)) && currentSplitPoints.size != splitsSet.size
    if (isPreviousSplitOperationFailed || generationRegions.size < nSplits) {
      val newSplits = splits.filterNot {
        split => currentSplitPoints.contains(new BytesKey(split))
      }
      for (split <- newSplits) {
        doAndRetryIfRegionIsNotServing {
          hBaseAdmin.split(pointsTableName.getName, split)
        }
      }
    }
  }

  def doAndRetryIfRegionIsNotServing(block: => Unit): Unit = {
    while(true) {
      try {
        block
        return
      } catch {
        case e: NotServingRegionException =>
          info("sleeping")
          Thread.sleep(500)
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
