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
import popeye.proto.{PackedPoints, Message}
import org.scalatest.exceptions.TestFailedException
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import nl.grons.metrics.scala.Meter

/**
 * @author Andrey Stepachev
 */
class PointsStorageSpec extends AkkaTestKitSpec("points-storage") with ShouldMatchers with MustMatchers with MockitoStubs {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val metricRegistry = new MetricRegistry()
  implicit val pointsStorageMetrics = new HBaseStorageMetrics(metricRegistry)

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
    val future = state.storage.writePoints(PackedPoints(events))
    val written = Await.result(future, 5 seconds)
    written should be(events.size)
    val points = hTable.getScanner(PointsStorage.PointsFamily).map(_.raw).flatMap { kv =>
      kv.map(state.storage.keyValueToPoint)
    }
    points.size should be(events.size)
    events.toList.sortBy(_.getTimestamp) should equal(points.toList.sortBy(_.getTimestamp))

    // write once more, we shold write using short path
    val future2 = state.storage.writePoints(PackedPoints(events))
    val written2 = Await.result(future2, 5 seconds)
    written2 should be(events.size)

  }

  ignore should "performance test" in {
    val state = new State

    val events = mkEvents(msgs = 4000)
    for (i <- 1 to 600) {
      val future = state.storage.writePoints(PackedPoints(events))
      val written = Await.result(future, 5 seconds)
      written should be(events.size)
    }
    printMeter(pointsStorageMetrics.writeHBasePoints)
  }

  private def printMeter(meter: Meter) {
    printf("             count = %d%n", meter.getCount)
    printf("         mean rate = %2.2f events/s%n", meter.getMeanRate)
    printf("     1-minute rate = %2.2f events/s%n", meter.getOneMinuteRate)
    printf("     5-minute rate = %2.2f events/s%n", meter.getFiveMinuteRate)
    printf("    15-minute rate = %2.2f events/s%n", meter.getFifteenMinuteRate)
  }

  class State {
    val id = new AtomicInteger(1)

    val uniqActor: TestActorRef[FixedUniqueIdActor] = TestActorRef(Props.apply(new FixedUniqueIdActor()))

    val metrics = setup(uniqActor, HBaseStorage.MetricKind, PopeyeTestUtils.names)
    val attrNames = setup(uniqActor, HBaseStorage.AttrNameKind, Seq("host"))
    val attrValues = setup(uniqActor, HBaseStorage.AttrValueKind, PopeyeTestUtils.hosts)
    val storage = new HBaseStorage(tableName, hTablePool, metrics, attrNames, attrValues, pointsStorageMetrics)

    def setup(actor: TestActorRef[FixedUniqueIdActor], kind: String, seq: Seq[String]): UniqueId = {
      val uniq = new UniqueIdImpl(HBaseStorage.UniqueIdMapping.get(kind).get, kind, actor)
      seq.map {
        item => actor.underlyingActor.add(ResolvedName(kind, item, uniq.toBytes(id.incrementAndGet())))
      }
      uniq
    }
  }

}
