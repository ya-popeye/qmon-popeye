package popeye.storage.hbase

import java.util.concurrent.atomic.AtomicInteger
import org.kiji.testing.fakehtable.FakeHTable
import org.apache.hadoop.hbase.client.{HTableInterface, HTableInterfaceFactory, HTablePool}
import org.apache.hadoop.conf.Configuration
import akka.testkit.TestActorRef
import akka.actor.{ActorSystem, Props}
import com.codahale.metrics.MetricRegistry
import scala.concurrent.ExecutionContext


class PointsStorageStub()
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

  val uniqActor: TestActorRef[InMemoryUniqueIdActor] = TestActorRef(Props.apply(new InMemoryUniqueIdActor()))

  val metrics = getUniqueId(uniqActor, HBaseStorage.MetricKind)
  val attrNames = getUniqueId(uniqActor, HBaseStorage.AttrNameKind)
  val attrValues = getUniqueId(uniqActor, HBaseStorage.AttrValueKind)
  private lazy val namespace: BytesKey = new BytesKey(Array[Byte](0, 0))
  val tsdbFormat = new TsdbFormat(new FixedTimeRangeID(namespace))
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

  private def getUniqueId(actor: TestActorRef[InMemoryUniqueIdActor], kind: String): UniqueId = {
    new UniqueIdImpl(HBaseStorage.UniqueIdMapping.get(kind).get, kind, actor)
  }
}

