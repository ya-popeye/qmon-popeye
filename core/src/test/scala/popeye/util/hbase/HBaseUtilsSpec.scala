package popeye.util.hbase

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import org.kiji.testing.fakehtable.FakeHTable
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.conf.Configuration
import popeye.util.hbase.HBaseUtils.ChunkedResults
import org.apache.hadoop.hbase.KeyValue


class HBaseUtilsSpec extends FlatSpec with ShouldMatchers {

  behavior of "HBaseUtils.getChunkedResults"

  it should "return correct chunks" in {
    val pool = getHTablePool("table")
    val utils = HBaseUtilsImpl(pool)
    val table = pool.getTable("table")
    val family = Array[Byte]('f')
    val qualifier = Array[Byte]('q')
    for (i <- 1 to 10) {
      val row = Array[Byte](i.toByte)
      val keyValue = new KeyValue(row, family, qualifier, Array[Byte](i.toByte))
      table.put(new Put(row).add(keyValue))
    }
    table.flushCommits()
    table.close()
    val startRow = Array[Byte](1)
    val stopRow = Array[Byte](11)
    val scan = new Scan(startRow, stopRow)
    val chunkedResults = utils.getChunkedResults("table", 3, scan)
    val flattenChunks = toSingleResult(chunkedResults)
    def getRowIndex(result: Result) = result.getRow()(0)
    flattenChunks.map(getRowIndex).toList should equal((1 to 10).map(_.toByte).toList)
  }

  private def toSingleResult(chunkedResults: ChunkedResults): Array[Result] = {
    val (chunk, next) = chunkedResults.getRows()
    chunk ++ next.map(toSingleResult).getOrElse(Array())
  }


  case class HBaseUtilsImpl(hTablePool: HTablePool) extends HBaseUtils

  private def getHTablePool(tableName: String) = {
    val hTable = new FakeHTable(tableName, desc = null)
    new HTablePool(new Configuration(), 1, new HTableInterfaceFactory {
      def releaseHTableInterface(table: HTableInterface) {}

      def createHTableInterface(config: Configuration, tableName: Array[Byte]): HTableInterface = hTable
    })
  }
}
