package popeye.storage.hbase

import akka.actor.Props
import akka.testkit.TestActorRef
import java.util.Random
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTableInterfaceFactory, HTableInterface, HTablePool}
import org.kiji.testing.fakehtable.FakeHTable
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import popeye.test.PopeyeTestUtils._
import popeye.test.{PopeyeTestUtils, MockitoStubs}
import popeye.transport.test.AkkaTestKitSpec
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.concurrent.Await
import scala.concurrent.duration._
import popeye.transport.proto.Message
import org.scalatest.exceptions.TestFailedException

/**
 * @author Andrey Stepachev
 */
class PointsStorageSpec extends AkkaTestKitSpec("points-storage") with ShouldMatchers with MustMatchers with MockitoStubs {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val random = new Random(1234)
  final val tableName = "my-table"
  final val hTable = new FakeHTable(tableName, desc = null)
  final val hTablePool = new HTablePool(new Configuration(), 1, new HTableInterfaceFactory {
    def releaseHTableInterface(table: HTableInterface) {}

    def createHTableInterface(config: Configuration, tableName: Array[Byte]): HTableInterface = hTable
  })

  behavior of "PointsStorage"

  it should "produce key values" in {
    val state = new State

    val events = mkEvents(msgs = 4)
    val future = state.storage.writePoints(events)
    val written = Await.result(future, 5 seconds)
    written should be(events.size)
    val points = hTable.getScanner(PointsStorage.PointsFamily).map(_.raw).flatMap { kv =>
      kv.map(state.storage.keyValueToPoint)
    }
    points.size should be(events.size)
    events.toList.sortBy(_.getTimestamp) should equal(points.toList.sortBy(_.getTimestamp))
  }

  class State {
    val id = new AtomicInteger(1)

    val uniqActor: TestActorRef[FixedUniqueIdActor] = TestActorRef(Props.apply(new FixedUniqueIdActor()))

    val metrics = setup(uniqActor, HBaseStorage.MetricKind, PopeyeTestUtils.names)
    val attrNames = setup(uniqActor, HBaseStorage.AttrNameKind, Seq("host"))
    val attrValues = setup(uniqActor, HBaseStorage.AttrValueKind, PopeyeTestUtils.hosts)
    val storage = new PointsStorage(tableName, hTablePool, metrics, attrNames, attrValues)

    def setup(actor: TestActorRef[FixedUniqueIdActor], kind: String, seq: Seq[String]): UniqueId = {
      val uniq = new UniqueIdImpl(HBaseStorage.UniqueIdMapping.get(kind).get, kind, actor)
      seq.map {
        item => actor.underlyingActor.add(ResolvedName(kind, item, uniq.toBytes(id.incrementAndGet())))
      }
      uniq
    }
  }

}
