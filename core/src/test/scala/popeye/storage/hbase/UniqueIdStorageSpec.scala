package popeye.storage.hbase

import java.util.Random
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTableInterfaceFactory, HTableInterface, HTablePool}
import org.kiji.testing.fakehtable.FakeHTable
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import popeye.test.MockitoStubs
import popeye.storage.hbase.HBaseStorage.{ResolvedName, QualifiedName}

/**
 * @author Andrey Stepachev
 */
class UniqueIdStorageSpec extends FlatSpec with ShouldMatchers with MockitoStubs {

  implicit val random = new Random(1234)
  final val tableName = "my-table"

  def hTablePool = {
    val hTable = new FakeHTable(tableName, desc = null)
    new HTablePool(new Configuration(), 1, new HTableInterfaceFactory {
      def releaseHTableInterface(table: HTableInterface) {}

      def createHTableInterface(config: Configuration, tableName: Array[Byte]): HTableInterface = hTable
    })
  }

  behavior of "UniqueIdStorage"

  it should "resolve id" in {
    val state = new State(hTablePool)

    val r1 = state.storage.findByName(Seq(
      state.metric1Name,
      state.metric2Name
    ))
    r1 should contain (state.metric1)
    r1.map(_.toQualifiedName) should (not contain state.metric2Name)
  }

  class State(htp: HTablePool) {
    val storage = new UniqueIdStorage(tableName, htp, HBaseStorage.UniqueIdMapping)
    var idStored = Map[BytesKey, ResolvedName]()
    var nameStored = Map[String, ResolvedName]()

    val metric1Name = QualifiedName(HBaseStorage.MetricKind, "metric.1")
    val metric1 = storage.registerName(metric1Name)
    val metric2Name = QualifiedName(HBaseStorage.MetricKind, "metric.2")


    def addMapping(resolvedName: ResolvedName): ResolvedName = {
      idStored = idStored.updated(resolvedName.id, resolvedName)
      nameStored = nameStored.updated(resolvedName.name, resolvedName)
      resolvedName
    }
  }

}
