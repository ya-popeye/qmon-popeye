package popeye.hadoop.bulkload

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import popeye.Logging

object BulkloadUtils extends Logging {


  def moveHFilesToHBase(hdfs: FileSystem,
                        outputPath: Path,
                        hBaseConfiguration: Configuration,
                        pointsTableName: TableName) = {
    // hbase needs rw access
    info("setting file permissions: granting rw access")
    setPermissionsRecursively(hdfs, outputPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    info("connecting to HBase")
    val hTable = new HTable(hBaseConfiguration, pointsTableName)
    try {
      info("calling LoadIncrementalHFiles.doBulkLoad")
      new LoadIncrementalHFiles(hBaseConfiguration).doBulkLoad(outputPath, hTable)
      info("bulk loading finished ")
    } finally {
      hTable.close()
    }
  }

  def setPermissionsRecursively(hdfs: FileSystem, path: Path, permission: FsPermission): Unit = {
    hdfs.setPermission(path, permission)
    if (hdfs.isDirectory(path)) {
      for (subPath <- hdfs.listStatus(path).map(_.getPath)) {
        setPermissionsRecursively(hdfs, subPath, permission)
      }
    }
  }
}
