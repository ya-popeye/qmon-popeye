package popeye.storage.hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.conf.Configuration

object CreateTsdbTables {

  def createTables(hbaseConfiguration: Configuration, pointsTableName: String, uidsTableName: String) {
    val tsdbTable = {
      val tsdbColumn = new HColumnDescriptor("t")
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(pointsTableName))
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
      try {
        hBaseAdmin.createNamespace(NamespaceDescriptor.create("popeye").build())
      } catch {
        case e: NamespaceExistException => // do nothing
      }
      val splits = (Byte.MinValue to Byte.MaxValue).map(i => Array(i.toByte)).toArray
      try {
        hBaseAdmin.createTable(tsdbTable, splits)
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

