package popeye.util.hbase

import com.codahale.metrics.MetricRegistry
import org.apache.hadoop.hbase.client.{Result, Scan, HTablePool, HTableInterface}
import popeye.{Instrumented, Logging}
import popeye.util.hbase.HBaseUtils.ChunkedResults

object HBaseUtils {

  def addOneIfNotMaximum(unsignedBytes: Array[Byte]): Array[Byte] = {
    val copy = unsignedBytes.clone()
    for (i <- (copy.length - 1) to 0 by -1) {
      if (copy(i) == 0xff.toByte) {
        copy(i) = 0x00.toByte
      } else {
        copy(i) = (copy(i) + 1).toByte
        return copy
      }
    }
    // not possible to add (all bytes are full)
    return unsignedBytes.clone()
  }

  def getChunkedResults(metrics: ChunkedResultsMetrics,
                        hTablePool: HTablePool,
                        tableName: String,
                        readChunkSize: Int,
                        scans: Seq[Scan]) = {
    require(scans.nonEmpty, "scans sequence is empty")
    new ChunkedResults(
      metrics,
      () => hTablePool.getTable(tableName),
      readChunkSize,
      scans.head,
      scans.tail
    )
  }

  class ChunkedResultsMetrics(name: String, override val metricRegistry: MetricRegistry) extends Instrumented {
    val tableCreationTime = metrics.timer(s"$name.table.creation.time")
    val scannerCreationTime = metrics.timer(s"$name.scanner.creation.time")
    val scannerCloseTime = metrics.timer(s"$name.scanner.close.time")
    val dummyNextCallTime = metrics.timer(s"$name.dummy.next.call.time")
    val dataNextCallTime = metrics.timer(s"$name.data.next.call.time")
  }

  class ChunkedResults(metrics: ChunkedResultsMetrics,
                       createTable: () => HTableInterface,
                       readChunkSize: Int,
                       scan: Scan,
                       nextScans: Seq[Scan],
                       skipFirstRow: Boolean = false) extends Logging {
    def getRows(): (Array[Result], Option[ChunkedResults]) = withHTable {
      table =>
        val scanner = metrics.scannerCreationTime.time {
          table.getScanner(scan)
        }
        val results =
          try {
            if (skipFirstRow) {
              metrics.dummyNextCallTime.time {
                scanner.next()
              }
            }
            metrics.dataNextCallTime.time {
              scanner.next(readChunkSize)
            }
          }
          finally {
            metrics.scannerCloseTime.time {
              scanner.close()
            }
          }
        val nextQuery =
          if (results.length < readChunkSize) {
            if (nextScans.nonEmpty) {
              val nextResults =
                new ChunkedResults(
                  metrics,
                  createTable,
                  readChunkSize,
                  nextScans.head,
                  nextScans.tail,
                  skipFirstRow = false
                )
              Some(nextResults)
            } else {
              None
            }
          } else {
            val lastRow = results.last.getRow
            val nextScan = new Scan(scan)
            nextScan.setStartRow(lastRow)
            val nextResults =
              new ChunkedResults(
                metrics,
                createTable,
                readChunkSize,
                nextScan,
                nextScans,
                skipFirstRow = true
              )
            Some(nextResults)
          }
        (results, nextQuery)
    }

    private def withHTable[U](body: (HTableInterface) => U): U = {
      debug("creating table")
      val hTable = metrics.tableCreationTime.time {
        createTable()
      }
      debug("table was created")
      try {
        debug("starting HTable task")
        val result = body(hTable)
        debug("HTable task succeeded")
        result
      } catch {
        case e: Exception =>
          error("HTable operation failed", e)
          throw e
      } finally {
        hTable.close()
      }
    }
  }

}