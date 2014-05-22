package popeye.storage.hbase

import java.util.Random
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTableInterfaceFactory, HTableInterface, HTablePool}
import org.kiji.testing.fakehtable.FakeHTable
import org.scalatest.{Matchers, FlatSpec}
import popeye.test.MockitoStubs
import popeye.storage.hbase.HBaseStorage.{ResolvedName, QualifiedName}

/**
 * @author Andrey Stepachev
 */
class UniqueIdStorageSpec extends FlatSpec with Matchers with MockitoStubs {

  implicit val random = new Random(1234)
  final val tableName = "my-table"

  def hTablePool = {
    val hTable = new FakeHTable(tableName, desc = null)
    new HTablePool(new Configuration(), 1, new HTableInterfaceFactory {
      def releaseHTableInterface(table: HTableInterface) {}

      def createHTableInterface(config: Configuration, tableName: Array[Byte]): HTableInterface = hTable
    })
  }

  behavior of "UniqueIdStorage.findByName"

  it should "resolve id" in {
    val storage = createStorage(hTablePool)
    val metric1Name = QualifiedName(HBaseStorage.MetricKind, "metric.1")
    val metric1 = storage.registerName(metric1Name)
    val metric2Name = QualifiedName(HBaseStorage.MetricKind, "metric.2")


    val r1 = storage.findByName(Seq(
      metric1Name,
      metric2Name
    ))
    r1 should contain(metric1)
    r1.map(_.toQualifiedName) should (not contain metric2Name)
  }

  behavior of "UniqueIdStorage.getSuggestions"

  it should "find name suggestions" in {
    val storage = createStorage(hTablePool)
    import HBaseStorage.MetricKind
    val names = Seq(
      ("aaa", MetricKind),
      ("aab", MetricKind),
      ("aab", MetricKind)
    )
    for ((name, kind) <- names) {
      storage.registerName(QualifiedName(kind, name))
    }
    storage.getSuggestions("aa", MetricKind, 10) should equal(Seq("aaa", "aab"))
    storage.getSuggestions("aab", MetricKind, 10) should equal(Seq("aab"))
  }

  it should "filter kinds" in {
    val storage = createStorage(hTablePool)
    import HBaseStorage.{MetricKind, AttrNameKind}
    val names = Seq(
      ("aaa", MetricKind),
      ("aab", MetricKind),
      ("aac", AttrNameKind)
    )
    for ((name, kind) <- names) {
      storage.registerName(QualifiedName(kind, name))
    }
    storage.getSuggestions("aa", MetricKind, 10) should equal(Seq("aaa", "aab"))
  }

  it should "return no more than 'limit' suggestions" in {
    val storage = createStorage(hTablePool)
    import HBaseStorage.MetricKind
    val names = Seq(
      ("aaa", MetricKind),
      ("aab", MetricKind),
      ("abb", MetricKind)
    )
    for ((name, kind) <- names) {
      storage.registerName(QualifiedName(kind, name))
    }
    storage.getSuggestions("a", MetricKind, limit = 2) should equal(Seq("aaa", "aab"))
  }

  def createStorage(htp: HTablePool) = new UniqueIdStorage(tableName, htp, HBaseStorage.UniqueIdMapping)

}
