package popeye.storage.hbase

import java.util.concurrent.atomic.AtomicInteger
import org.kiji.testing.fakehtable.FakeHTable
import org.apache.hadoop.hbase.client.{HTableInterface, HTableInterfaceFactory, HTablePool}
import org.apache.hadoop.conf.Configuration
import akka.testkit.TestActorRef
import akka.actor.{ActorSystem, Props}
import com.codahale.metrics.MetricRegistry
import scala.concurrent.ExecutionContext

object PointsStorageStub {
  val timeRangeIdMapping: FixedGenerationId = new FixedGenerationId(0)
}

class PointsStorageStub(timeRangeIdMapping: GenerationIdMapping = PointsStorageStub.timeRangeIdMapping,
                        shardAttrs: Set[String] = Set("host"))
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

  val uniqueId = new UniqueIdImpl(uniqActor)
  val tsdbFormat = new TsdbFormat(timeRangeIdMapping, shardAttrs)
  val storage = new HBaseStorage(
    tableName,
    hTablePool,
    uniqueId,
    tsdbFormat,
    pointsStorageMetrics,
    readChunkSize = 10
  )
}

