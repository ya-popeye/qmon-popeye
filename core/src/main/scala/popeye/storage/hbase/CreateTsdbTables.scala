package popeye.storage.hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.conf.Configuration

object CreateTsdbTables {

  val tsdbTable = {
    val tsdbColumn = new HColumnDescriptor("t")
    val tableDescriptor = new HTableDescriptor(popeyeTableName("tsdb"))
    tableDescriptor.addFamily(tsdbColumn)
    tableDescriptor
  }

  val tsdbUidTable = {
    val nameColumn = new HColumnDescriptor("name")
    val idColumn = new HColumnDescriptor("id")
    val tableDescriptor = new HTableDescriptor(popeyeTableName("tsdb-uid"))
    tableDescriptor.addFamily(nameColumn)
    tableDescriptor.addFamily(idColumn)
    tableDescriptor
  }

  def popeyeTableName(name: String) = TableName.valueOf("popeye", name)

  def createTables(hbaseConfiguration: Configuration) = {
    val hBaseAdmin = new HBaseAdmin(hbaseConfiguration)
    hBaseAdmin.createNamespace(NamespaceDescriptor.create("popeye").build())
    val splits = (Byte.MinValue to Byte.MaxValue).map(i => Array(i.toByte)).toArray
    hBaseAdmin.createTable(tsdbTable, splits)
    hBaseAdmin.createTable(tsdbUidTable)
    hBaseAdmin.close()
  }

  def main(args: Array[String]) {
    val zookeeperQuorum: String = args(0)
    val zookeeperPort: Int = 2181
    val hbaseConfiguration = HBaseConfiguration.create
    hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    hbaseConfiguration.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort)
    createTables(hbaseConfiguration)
  }
}

