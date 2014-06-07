package popeye.storage.hbase

import java.util.Random
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTableInterfaceFactory, HTableInterface, HTablePool}
import org.kiji.testing.fakehtable.FakeHTable
import org.scalatest.{Matchers, FlatSpec}
import popeye.test.MockitoStubs
import popeye.storage.hbase.HBaseStorage._
import org.scalatest.OptionValues._
import popeye.test.PopeyeTestUtils.bytesKey

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

  val defaultGenerationId = bytesKey(1)

  behavior of "UniqueIdStorage.findByName"

  it should "resolve id" in {
    val storage = createStorage(hTablePool)
    val metric1Name = QualifiedName(MetricKind, defaultGenerationId, "metric.1")
    val metric1 = storage.registerName(metric1Name)
    val metric2Name = QualifiedName(MetricKind, defaultGenerationId, "metric.2")

    val r1 = storage.findByName(Seq(
      metric1Name,
      metric2Name
    ))
    r1 should contain(metric1)
    r1.map(_.toQualifiedName) should (not contain metric2Name)
  }

  it should "register names in generations" in {
    val storage = createStorage(hTablePool)
    val metricName = QualifiedName(MetricKind, bytesKey(100), "metric")
    storage.registerName(metricName)
    val resolvedNames = storage.findByName(Seq(metricName))
    resolvedNames.size should equal(1)
    resolvedNames.head.generationId should equal(metricName.generationId)
  }

  it should "find by name" in {
    val storage = createStorage(hTablePool)
    val metricName = QualifiedName(MetricKind, bytesKey(1), "metric")
    val resolvedName = storage.registerName(metricName)
    val resolvedNameOption = storage.findByName(metricName)
    resolvedNameOption.value should equal(resolvedName)
  }

  it should "not find name defined in a different generation" in {
    val storage = createStorage(hTablePool)
    val name: String = "metric"
    val resolvedName = storage.registerName(QualifiedName(MetricKind, bytesKey(1), name))
    val resolvedNameOption = storage.findByName(QualifiedName(MetricKind, bytesKey(0), name))
    resolvedNameOption should be(None)
  }

  it should "find by id" in {
    val storage = createStorage(hTablePool)
    val metricName = QualifiedName(MetricKind, bytesKey(1), "metric")
    val metricId = storage.registerName(metricName).toQualifiedId
    val resolvedNames = storage.findById(Seq(metricId, metricId.copy(generationId = bytesKey(0))))
    resolvedNames.size should equal(1)
    resolvedNames.head.toQualifiedName should equal(metricName)
  }

  behavior of "UniqueIdStorage.getSuggestions"

  it should "find name suggestions" in {
    val storage = createStorage(hTablePool)
    import HBaseStorage.MetricKind
    val names = Seq(
      ("aaa", MetricKind),
      ("aab", MetricKind)
    )
    for ((name, kind) <- names) {
      storage.registerName(QualifiedName(kind, defaultGenerationId, name))
    }
    storage.getSuggestions(MetricKind, defaultGenerationId, "aa", 10) should equal(Seq("aaa", "aab"))
    storage.getSuggestions(MetricKind, defaultGenerationId, "aab", 10) should equal(Seq("aab"))
  }

  it should "be aware of generations" in {
    val storage = createStorage(hTablePool)
    import HBaseStorage.MetricKind
    val names = Seq(
      ("aaa", bytesKey(0), MetricKind),
      ("aab", bytesKey(1), MetricKind)
    )
    for ((name, generationId, kind) <- names) {
      storage.registerName(QualifiedName(kind, generationId, name))
    }
    storage.getSuggestions(MetricKind, bytesKey(0), "aa", 10) should equal(Seq("aaa"))
    storage.getSuggestions(MetricKind, bytesKey(0), "aab", 10) should equal(Seq())
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
      storage.registerName(QualifiedName(kind, defaultGenerationId, name))
    }
    storage.getSuggestions(MetricKind, defaultGenerationId, "aa", 10) should equal(Seq("aaa", "aab"))
  }

  it should "return no more than 'limit' suggestions" in {
    val storage = createStorage(hTablePool)
    import HBaseStorage.MetricKind
    val names = Seq(
      ("aaa", MetricKind),
      ("aab", MetricKind),
      ("abc", MetricKind)
    )
    for ((name, kind) <- names) {
      storage.registerName(QualifiedName(kind, defaultGenerationId, name))
    }
    storage.getSuggestions(MetricKind, defaultGenerationId, "a", limit = 2) should equal(Seq("aaa", "aab"))
  }

  def createStorage(htp: HTablePool) = new UniqueIdStorage(tableName, htp, generationIdWidth = 1)

}
