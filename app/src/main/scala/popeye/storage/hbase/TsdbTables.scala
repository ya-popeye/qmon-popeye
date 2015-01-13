package popeye.storage.hbase

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import popeye.Logging
import popeye.paking.TsdbRegionObserver
import scala.collection.JavaConverters._

object TsdbTables extends Logging {

  def createTables(hbaseConfiguration: Configuration,
                   pointsTableName: String,
                   uidsTableName: String,
                   coprocessorJarPathOption: Option[Path]) {
    def getNamespace(tableName: String) = TableName.valueOf(tableName).getNamespaceAsString

    val namespaces = Seq(pointsTableName, uidsTableName).map(getNamespace)

    val tsdbTable = {
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(pointsTableName))
      val tsdbColumn = new HColumnDescriptor("t")
      tsdbColumn.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      tableDescriptor.addFamily(tsdbColumn)
      for (coprocessorJarPath <- coprocessorJarPathOption) {
        tableDescriptor.addCoprocessor(
          classOf[TsdbRegionObserver].getCanonicalName,
          coprocessorJarPath,
          Coprocessor.PRIORITY_USER,
          Map.empty.asJava
        )
      }
      tableDescriptor
    }

    val tsdbUidTable = {
      val nameColumn = new HColumnDescriptor("name")
      val idColumn = new HColumnDescriptor("id")
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(uidsTableName))
      tableDescriptor.addFamily(nameColumn)
      tableDescriptor.addFamily(idColumn)
      tableDescriptor
    }

    val hBaseAdmin = new HBaseAdmin(hbaseConfiguration)
    try {
      for (namespace <- namespaces) {
        try {
          info(f"creating namespace $namespace")
          hBaseAdmin.createNamespace(NamespaceDescriptor.create(namespace).build())
          info(f"namespace $namespace was created")
        } catch {
          case e: NamespaceExistException => // do nothing
        }
      }
      try {
        info(f"creating points table $tsdbTable")
        hBaseAdmin.createTable(tsdbTable)
        info(f"points table $tsdbTable was created")
      } catch {
        case e: TableExistsException => // do nothing
      }
      try {
        info(f"creating uids table $tsdbUidTable")
        hBaseAdmin.createTable(tsdbUidTable)
        info(f"uids table $tsdbUidTable was created")
      } catch {
        case e: TableExistsException => // do nothing
      }
    } finally {
      hBaseAdmin.close()
    }
  }
}

