package popeye.paking

import java.util

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.coprocessor.{RegionCoprocessorEnvironment, ObserverContext, BaseRegionObserver}
import org.apache.hadoop.hbase.regionserver.{ScanType, InternalScanner, Store}
import org.apache.hadoop.hbase.util.Bytes
import popeye.storage.hbase.TsdbFormat

import scala.collection.JavaConverters._
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

class TsdbRegionObserver extends BaseRegionObserver {
  val log = LogFactory.getLog(classOf[TsdbRegionObserver])

  override def preCompact(e: ObserverContext[RegionCoprocessorEnvironment],
                          store: Store,
                          scanner: InternalScanner,
                          scanType: ScanType): InternalScanner = {
    new PackingScanner(scanner, TsdbFormat.rowPacker, log)
  }
}

class PackingScanner(scanner: InternalScanner, rowPacker: RowPacker, log: Log) extends InternalScanner {
  override def next(results: util.List[Cell]): Boolean = {
    val hasNextRows = scanner.next(results)
    if (results.isEmpty) {
      return hasNextRows
    }
    val firstCell = results.get(0)
    val rowArray = firstCell.getRowArray
    val rowOffset = firstCell.getRowOffset
    val rowLength = firstCell.getRowLength
    if (rowLength <= TsdbFormat.valueTypeIdOffset) {
      // skip bad row
      if (log.isDebugEnabled) {
        val row = Bytes.toStringBinary(rowArray, rowOffset, rowLength)
        log.debug(f"bad row: $row")
      }
      return hasNextRows
    }
    if (rowArray(rowOffset + TsdbFormat.valueTypeIdOffset) == TsdbFormat.ValueTypes.SingleValueTypeStructureId) {
      val cell = rowPacker.packRow(results.asScala)
      results.clear()
      results.add(cell)
    }
    hasNextRows
  }

  override def next(result: util.List[Cell], limit: Int): Boolean = {
    next(result)
  }

  override def close(): Unit = {
    scanner.close()
  }
}
