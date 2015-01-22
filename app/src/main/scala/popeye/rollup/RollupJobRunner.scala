package popeye.rollup

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{TableName, KeyValue}
import org.apache.hadoop.hbase.client.{HTable, Scan, HBaseAdmin}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableMapReduceUtil}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import popeye.Logging
import popeye.hadoop.bulkload.BulkloadUtils
import popeye.rollup.RollupMapperEngine.RollupStrategy
import popeye.rollup.RollupMapperEngine.RollupStrategy.{DayRollup, HourRollup}
import popeye.storage.hbase.{TsdbFormatConfig, TsdbFormat}
import popeye.util.ARM

class RollupJobRunner(hBaseConfiguration: Configuration,
                      pointsTableName: TableName,
                      hadoopConfiguration: Configuration,
                      restoreDirParent: Path,
                      outputPath: Path,
                      jarPaths: Seq[Path],
                      tsdbFormatConfig: TsdbFormatConfig) extends Logging {
  def doRollup(generationId: Short,
               rollupStrategy: RollupStrategy,
               baseStartTime: Int,
               baseStopTime: Int): Unit = {
    checkGenerationRange(generationId, baseStartTime)
    checkGenerationRange(generationId, baseStopTime)
    checkRangeBoundary(rollupStrategy, baseStartTime)
    checkRangeBoundary(rollupStrategy, baseStopTime)
    val arm = for {
      hbaseAdmin <- hBaseAdminResource(hBaseConfiguration)
      snapshotName <- hBaseSnapshotResource(hbaseAdmin, pointsTableName)
      hTable <- hTableResource(hBaseConfiguration, pointsTableName)
      hdfs <- hdfsResource(hadoopConfiguration)
      restoreDir <- tempHdfsDirectoryResource(hdfs, restoreDirParent)
    } yield {
      info("resources were successfully created")
      val job = Job.getInstance(hadoopConfiguration)
      val downsamplingResolutionId = {
        import TsdbFormat.DownsamplingResolution._
        rollupStrategy match {
          case HourRollup => noDownsamplingResolutionId
          case DayRollup => getId(Hour)
        }
      }
      val currentTime = System.currentTimeMillis()
      RollupMapperEngine.setConfiguration(job.getConfiguration, rollupStrategy, tsdbFormatConfig, currentTime)
      val scan = new Scan()
      val filter = new TsdbPointsFilter(
        generationId,
        downsamplingResolutionId,
        TsdbFormat.ValueTypes.SingleValueTypeStructureId,
        baseStartTime,
        baseStopTime
      )
      scan.setFilter(filter)
      TableMapReduceUtil.initTableSnapshotMapperJob(
        snapshotName,
        scan,
        classOf[RollupMapper],
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        job,
        false,
        restoreDir
      )
      HFileOutputFormat2.configureIncrementalLoad(job, hTable)
      // HFileOutputFormat2.configureIncrementalLoad abuses tmpjars
      job.getConfiguration.unset("tmpjars")
      FileOutputFormat.setOutputPath(job, outputPath)

      for (path <- jarPaths) {
        job.addFileToClassPath(path)
      }
      info("job configured, starting job")
      val success = job.waitForCompletion(false)
      if (success) {
        info("job succeeded, moving files to hbase")
        BulkloadUtils.moveHFilesToHBase(hdfs, outputPath, hBaseConfiguration, pointsTableName)
        info("files were moved")
      } else {
        info("job failed")
      }
    }

    arm.run()
  }

  def hBaseAdminResource(hBaseConfiguration: Configuration) =
    ARM.closableResource(() => new HBaseAdmin(hBaseConfiguration))

  def hBaseSnapshotResource(hBaseAdmin: HBaseAdmin, tableName: TableName) = ARM.resource[String](
    () => {
      val snapshotName = UUID.randomUUID().toString
      hBaseAdmin.snapshot(snapshotName, tableName)
      snapshotName
    },
    snapshotName => hBaseAdmin.deleteSnapshot(snapshotName)
  )

  def tempHdfsDirectoryResource(hdfs: FileSystem, parentPath: Path) = ARM.resource[Path](
    () => {
      val tempDirName = UUID.randomUUID().toString.replaceAll("-", "")
      val tempDirPath = new Path(parentPath, tempDirName)
      hdfs.mkdirs(tempDirPath)
      tempDirPath
    },
    tempDirPath => hdfs.delete(tempDirPath, true)
  )

  def hTableResource(hBaseConfiguration: Configuration, tableName: TableName) =
    ARM.closableResource(() => new HTable(hBaseConfiguration, tableName))

  def hdfsResource(hadoopConfiguration: Configuration) =
    ARM.closableResource(() => FileSystem.newInstance(hadoopConfiguration))

  def checkGenerationRange(generationId: Short, timestamp: Int) {
    val timestampGenerationId = tsdbFormatConfig.generationIdMapping.backwardIterator(timestamp).next().id
    require(timestampGenerationId == generationId, f"timestamp $timestamp is out of generation ($generationId) range")
  }

  def checkRangeBoundary(rollupStrategy: RollupStrategy, baseStartTime: Int) {
    require(
      rollupStrategy.isTimestampRangeBoundaryAcceptable(baseStartTime),
      f"$rollupStrategy do not accept $baseStartTime as a range boundary"
    )
  }
}
