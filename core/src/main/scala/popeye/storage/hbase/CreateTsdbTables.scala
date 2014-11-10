package popeye.storage.hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding

object CreateTsdbTables {

  def createTables(hbaseConfiguration: Configuration, pointsTableName: String, uidsTableName: String) {
    def getNamespace(tableName: String) = TableName.valueOf(tableName).getNamespaceAsString

    val namespaces = Seq(pointsTableName, uidsTableName).map(getNamespace)

    val tsdbTable = {
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(pointsTableName))
      val tsdbColumn = new HColumnDescriptor("t")
      tsdbColumn.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      tableDescriptor.addFamily(tsdbColumn)
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
          hBaseAdmin.createNamespace(NamespaceDescriptor.create(namespace).build())
        } catch {
          case e: NamespaceExistException => // do nothing
        }
      }
      try {
        hBaseAdmin.createTable(tsdbTable)
      } catch {
        case e: TableExistsException => // do nothing
      }
      try {
        hBaseAdmin.createTable(tsdbUidTable)
      } catch {
        case e: TableExistsException => // do nothing
      }
    } finally {
      hBaseAdmin.close()
    }
  }

}

