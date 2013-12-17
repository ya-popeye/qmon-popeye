package popeye.util.hbase

import org.apache.hadoop.hbase.client.{Result, Scan, HTablePool, HTableInterface}
import popeye.Logging
import popeye.util.hbase.HBaseUtils.ChunkedResults

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

  def getChunkedResults(tableName: String, readChunkSize: Int, scan: Scan) =
    new ChunkedResults(tableName, readChunkSize, scan, this)

}

object HBaseUtils {

  class ChunkedResults(tableName: String,
                       readChunkSize: Int,
                       scan: Scan,
                       utils: HBaseUtils,
                       skipFirstRow: Boolean = false) {
    def getRows(): (Array[Result], Option[ChunkedResults]) = utils.withHTable(tableName) {
      table =>
        val scanner = table.getScanner(scan)
        if (skipFirstRow) {
          try {scanner.next()}
          finally {scanner.close()}
        }
        val results =
          try {scanner.next(readChunkSize)}
          finally {scanner.close()}
        val nextQuery =
          if (results.length < readChunkSize) {
            None
          } else {
            val lastRow = results.last.getRow
            val nextScan = new Scan(scan)
            nextScan.setStartRow(lastRow)
            val nextResults =
              new ChunkedResults(
                tableName,
                readChunkSize,
                nextScan,
                utils,
                skipFirstRow = true
              )
            Some(nextResults)
          }
        (results, nextQuery)
    }
  }

}