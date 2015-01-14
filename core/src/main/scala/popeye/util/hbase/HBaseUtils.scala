package popeye.util.hbase

import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import org.apache.hadoop.hbase.client.{Result, Scan, HTablePool, HTableInterface}
import popeye.{ImmutableIterator, Instrumented, Logging}

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
                        scans: Seq[Scan]): ImmutableIterator[Array[Result]] = {
    require(scans.nonEmpty, "scans sequence is empty")
    new ChunkedResults(
      metrics,
      () => hTablePool.getTable(tableName),
      readChunkSize,
      scans.toList
    )
  }

  class ChunkedResultsMetrics(name: String, override val metricRegistry: MetricRegistry) extends Instrumented {
    val tableCreationTime = metrics.timer(s"$name.table.creation.time")
    val scannerCreationTime = metrics.timer(s"$name.scanner.creation.time")
    val scannerCloseTime = metrics.timer(s"$name.scanner.close.time")
    val dummyNextCallTime = metrics.timer(s"$name.dummy.next.call.time")
    val dataNextCallTime = metrics.timer(s"$name.data.next.call.time")
    val scanCount = metrics.histogram(s"$name.scan.count")
    val scanTime = metrics.timer(s"$name.scan.time")
    val totalScanTimeMillis = metrics.histogram(s"$name.scan.total.time.millis")
  }

  case class ChunkedResults(metrics: ChunkedResultsMetrics,
                            createTable: () => HTableInterface,
                            readChunkSize: Int,
                            scans: List[Scan],
                            skipFirstRow: Boolean = false,
                            totalScanCount: Int = 0,
                            totalScanTimeMillis: Long = 0) extends ImmutableIterator[Array[Result]] with Logging {

    def next: Option[(Array[Result], ImmutableIterator[Array[Result]])] = {
      if (scans.isEmpty) {
        metrics.scanCount.update(totalScanCount)
        metrics.totalScanTimeMillis.update(totalScanTimeMillis)
      }
      scans.headOption.map {
        scan =>
          val scanTimer = metrics.scanTime.timerContext()
          val results = fetchResults(scan)
          val scanTime = TimeUnit.NANOSECONDS.toMillis(scanTimer.stop())
          val nextChunkedResultsWithoutTotals =
            if (results.length < readChunkSize) {
              copy(
                scans = scans.tail,
                skipFirstRow = false
              )
            } else {
              val lastRow = results.last.getRow
              val nextScan = new Scan(scan)
              nextScan.setStartRow(lastRow)
              copy(
                scans = nextScan :: scans.tail,
                skipFirstRow = true
              )
            }
          val nextChunkedResults = nextChunkedResultsWithoutTotals.copy(
            totalScanCount = totalScanCount + 1,
            totalScanTimeMillis = totalScanTimeMillis + scanTime
          )
          (results, nextChunkedResults)
      }
    }

    def fetchResults(scan: Scan) = withHTable {
      table =>
        scan.setCaching(readChunkSize + 1)
        val scanner = metrics.scannerCreationTime.time {
          table.getScanner(scan)
        }
        try {
          if (skipFirstRow) {
            metrics.dummyNextCallTime.time {
              scanner.next()
            }
          }
          val results = metrics.dataNextCallTime.time {
            scanner.next(readChunkSize)
          }
          results
        } finally {
          metrics.scannerCloseTime.time {
            scanner.close()
          }
        }
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