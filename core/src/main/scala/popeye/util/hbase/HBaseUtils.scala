package popeye.util.hbase

import org.apache.hadoop.hbase.client.{HTablePool, HTableInterface}
import popeye.Logging
import java.nio.charset.Charset
import java.nio.ByteBuffer

private[popeye] trait HBaseUtils extends Logging {

  def hTablePool: HTablePool

  @inline
  protected def withHTable[U](tableName: String)(body: (HTableInterface) => U): U = {
    log.debug("withHTable - trying to get HTable {}", tableName)
    val hTable = hTablePool.getTable(tableName)
    log.debug("withHTable - got HTable {}", tableName)
    try {
      body(hTable)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    } finally {
      hTable.close()
    }
  }

}