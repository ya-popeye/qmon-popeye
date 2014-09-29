package popeye.storage.hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding

object CreateTsdbTables {

  def createTables(hbaseConfiguration: Configuration, pointsTableName: String, uidsTableName: String) {
    val namespace = {
      val pointsTokens = pointsTableName.split(":")
      require(pointsTokens.size == 2, f"namespace not specified or wrong format: $pointsTableName")
      val uidsTokens = pointsTableName.split(":")
      require(uidsTokens.size == 2, f"namespace not specified or wrong format: $pointsTableName")
      require(pointsTokens(0) == uidsTokens(0), f"namespaces mismatch: $pointsTableName $uidsTableName")
      pointsTokens(0)
    }

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
      try {
        hBaseAdmin.createNamespace(NamespaceDescriptor.create(namespace).build())
      } catch {
        case e: NamespaceExistException => // do nothing
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

