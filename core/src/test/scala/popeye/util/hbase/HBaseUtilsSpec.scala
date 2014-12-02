package popeye.util.hbase

import com.codahale.metrics.MetricRegistry
import org.scalatest.{Matchers, FlatSpec}
import org.kiji.testing.fakehtable.FakeHTable
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.conf.Configuration
import popeye.util.hbase.HBaseUtils.{ChunkedResultsMetrics, ChunkedResults}
import org.apache.hadoop.hbase.KeyValue


class HBaseUtilsSpec extends FlatSpec with Matchers {

  behavior of "HBaseUtils.getChunkedResults"

  it should "return correct chunks" in {
    val pool = getHTablePool("table")
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
    val firstScan = {
      val startRow = Array[Byte](1)
      val stopRow = Array[Byte](5)
      new Scan(startRow, stopRow)
    }
    val secondScan = {
      val startRow = Array[Byte](5)
      val stopRow = Array[Byte](11)
      new Scan(startRow, stopRow)
    }
    val metrics = new ChunkedResultsMetrics("chunked", new MetricRegistry)
    val chunkedResults = HBaseUtils.getChunkedResults(metrics, pool, "table", 3, Seq(firstScan, secondScan))
    val flattenChunks = toSingleResult(chunkedResults)
    def getRowIndex(result: Result) = result.getRow()(0)
    flattenChunks.map(getRowIndex).toList should equal((1 to 10).map(_.toByte).toList)
  }

  behavior of "HBaseUtils.addOneIfNotMaximum"

  it should "add one" in {
    val zero = Array[Byte](0x00, 0x00)
    val one = Array[Byte](0x00, 0x01)
    HBaseUtils.addOneIfNotMaximum(zero) should equal(one)
  }

  it should "carry" in {
    val n255 = Array[Byte](0x00, 0xff.toByte)
    val n256 = Array[Byte](0x01, 0x00)
    HBaseUtils.addOneIfNotMaximum(n255) should equal(n256)
  }

  it should "not add if maximum" in {
    val maximum = Array[Byte](0xff.toByte, 0xff.toByte)
    HBaseUtils.addOneIfNotMaximum(maximum) should equal(maximum)
  }

  it should "copy" in {
    val maximum = Array[Byte](0xff.toByte, 0xff.toByte)
    HBaseUtils.addOneIfNotMaximum(maximum) should not(be theSameInstanceAs maximum)
    val zero = Array[Byte](0x00, 0x00)
    HBaseUtils.addOneIfNotMaximum(zero) should not(be theSameInstanceAs zero)
  }

  private def toSingleResult(chunkedResults: ChunkedResults): Array[Result] = {
    val (chunk, next) = chunkedResults.getRows()
    chunk ++ next.map(toSingleResult).getOrElse(Array())
  }

  private def getHTablePool(tableName: String) = {
    val hTable = new FakeHTable(tableName, desc = null)
    new HTablePool(new Configuration(), 1, new HTableInterfaceFactory {
      def releaseHTableInterface(table: HTableInterface) {}

      def createHTableInterface(config: Configuration, tableName: Array[Byte]): HTableInterface = hTable
    })
  }
}
