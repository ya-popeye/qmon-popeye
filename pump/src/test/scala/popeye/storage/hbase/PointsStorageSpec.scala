package popeye.storage.hbase

import java.util
import java.util.Random
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseIOException
import org.apache.hadoop.hbase.client.{HTableInterfaceFactory, Put, HTableInterface, HTablePool}
import org.apache.hadoop.hbase.util.Bytes
import org.kiji.testing.fakehtable.FakeHTable
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import popeye.test.{PopeyeTestUtils, MockitoStubs}
import popeye.test.PopeyeTestUtils._
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.concurrent.duration._
import scala.concurrent.{Promise, Await}
import popeye.transport.test.AkkaTestKitSpec
import akka.actor.{ActorRef, Props}
import akka.testkit.{TestActorRef, TestProbe}

/**
 * @author Andrey Stepachev
 */
class PointsStorageSpec extends AkkaTestKitSpec("points-storage") with ShouldMatchers with MockitoStubs {

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

    val events = mkEvents()
    val future = state.storage.writePoints(events)
    val written = Await.result(future, 5 seconds)
    written should be(events.size)
    val points = hTable.getScanner(PointsStorage.PointsFamily).map(_.raw).flatMap { kv =>
      kv.map(state.storage.keyValueToPoint)
    }
    points.size should be(events.size)
  }

//  it should "handle failures" in {
//    val state = new State
//
//    state.hTable.put(anyListOf(classOf[Put])) throws new HBaseIOException("Some cryptic hbase exception")
//
//    val future = state.storage.writePoints(mkEvents())
//    intercept[HBaseIOException] {
//      Await.result(future, 5 seconds)
//    }
//    verify(state.hTablePool, atLeastOnce()).getTable(any[String]())
//    verify(state.hTable, atLeastOnce()).close()
//    verify(state.hTable, never()).flushCommits()
//  }

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
