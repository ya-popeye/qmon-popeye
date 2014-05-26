package popeye.storage.hbase

import java.util.concurrent.atomic.AtomicInteger
import org.kiji.testing.fakehtable.FakeHTable
import org.apache.hadoop.hbase.client.{HTableInterface, HTableInterfaceFactory, HTablePool}
import org.apache.hadoop.conf.Configuration
import akka.testkit.TestActorRef
import akka.actor.{ActorSystem, Props}
import com.codahale.metrics.MetricRegistry
import scala.concurrent.ExecutionContext


class PointsStorageStub(metricNames: Seq[String] = Seq(),
                        attributeNames: Seq[String] = Seq(),
                        attributeValues: Seq[String] = Seq())
                       (implicit val actorSystem: ActorSystem,
                        implicit val executionContext: ExecutionContext) {
  val pointsStorageMetrics = new HBaseStorageMetrics("hbase", new MetricRegistry())
  val id = new AtomicInteger(1)
  val tableName = "tsdb"
  val hTable = new FakeHTable(tableName, desc = null)
  val hTablePool = new HTablePool(new Configuration(), 1, new HTableInterfaceFactory {
    def releaseHTableInterface(table: HTableInterface) {}

    def createHTableInterface(config: Configuration, tableName: Array[Byte]): HTableInterface = hTable
  })

  val uniqActor: TestActorRef[FixedUniqueIdActor] = TestActorRef(Props.apply(new FixedUniqueIdActor()))

  val metrics = setup(uniqActor, HBaseStorage.MetricKind, metricNames)
  val attrNames = setup(uniqActor, HBaseStorage.AttrNameKind, attributeNames)
  val attrValues = setup(uniqActor, HBaseStorage.AttrValueKind, attributeValues)
  val tsdbFormat = new TsdbFormat(new FixedTimeRangeID(new BytesKey(Array[Byte](0, 0))))
  val storage = new HBaseStorage(
    tableName,
    hTablePool,
    metrics,
    attrNames,
    attrValues,
    tsdbFormat,
    pointsStorageMetrics,
    readChunkSize = 10
  )

  private def setup(actor: TestActorRef[FixedUniqueIdActor], kind: String, seq: Seq[String]): UniqueId = {
    val uniq = new UniqueIdImpl(HBaseStorage.UniqueIdMapping.get(kind).get, kind, actor)
    seq.map {
      item => actor.underlyingActor.add(HBaseStorage.ResolvedName(kind, item, uniq.toBytes(id.incrementAndGet())))
    }
    uniq
  }

  private def addUniqId(name: String, uniq: UniqueId) =
    uniqActor.underlyingActor.add(HBaseStorage.ResolvedName(uniq.kind, name, uniq.toBytes(id.incrementAndGet())))

  def addMetric(name: String) = addUniqId(name, metrics)

  def addAttrName(name: String) = addUniqId(name, attrNames)

  def addAttrValue(name: String) = addUniqId(name, attrValues)
}

