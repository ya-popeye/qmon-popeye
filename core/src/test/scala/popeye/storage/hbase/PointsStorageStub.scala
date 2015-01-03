package popeye.storage.hbase

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import akka.dispatch.ExecutionContexts
import org.apache.hadoop.hbase.DaemonThreadFactory
import org.kiji.testing.fakehtable.FakeHTable
import org.apache.hadoop.hbase.client.{HTableInterface, HTableInterfaceFactory, HTablePool}
import org.apache.hadoop.conf.Configuration
import akka.testkit.TestActorRef
import akka.actor.{Actor, ActorSystem, Props}
import com.codahale.metrics.MetricRegistry
import scala.concurrent.ExecutionContext

object PointsStorageStub {
  val timeRangeIdMapping: FixedGenerationId = new FixedGenerationId(0)
}

class PointsStorageStub(timeRangeIdMapping: GenerationIdMapping = PointsStorageStub.timeRangeIdMapping,
                        shardAttrs: Set[String] = Set("host"))
                       (implicit val actorSystem: ActorSystem,
                        implicit val executionContext: ExecutionContext) {
  private val metricRegistry = new MetricRegistry()
  val pointsStorageMetrics = new HBaseStorageMetrics("hbase", metricRegistry)
  val id = new AtomicInteger(1)
  val pointsTableName = "tsdb"
  val uidTableName = "tsdb-uid"
  val pointsTable = new FakeHTable(pointsTableName, desc = null)
  val uIdHTable = new FakeHTable(uidTableName, desc = null)
  val hTablePool = createHTablePool(pointsTable)
  val uIdHTablePool = createHTablePool(uIdHTable)

  val uniqueIdStorage = {
    val metrics = new UniqueIdStorageMetrics("uid", metricRegistry)
    new UniqueIdStorage(uidTableName, uIdHTablePool, metrics)
  }

  def uniqActorProps = {
    val executor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("unique id actor"))
    val executionContext = ExecutionContexts.fromExecutor(executor)
    Props.apply(UniqueIdActor(uniqueIdStorage, executionContext))
  }

  def uniqActor: TestActorRef[Actor] = TestActorRef(uniqActorProps)

  def uniqueId = new UniqueIdImpl(uniqActor, new UniqueIdMetrics("uniqueid", metricRegistry))
  val tsdbFormat = new TsdbFormat(timeRangeIdMapping, shardAttrs)
  val storage = new HBaseStorage(
    pointsTableName,
    hTablePool,
    uniqueId,
    tsdbFormat,
    pointsStorageMetrics,
    readChunkSize = 10
  )

  def createHTablePool(hTable: HTableInterface): HTablePool = {
    new HTablePool(new Configuration(), 1, new HTableInterfaceFactory {
      def releaseHTableInterface(table: HTableInterface) {}

      def createHTableInterface(config: Configuration, tableNameBytes: Array[Byte]): HTableInterface = hTable
    })
  }
}

