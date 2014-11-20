package popeye.storage.hbase

import java.util
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Props}
import com.codahale.metrics.MetricRegistry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, HTableInterface, HTableInterfaceFactory, HTablePool}
import org.kiji.testing.fakehtable.FakeHTable
import popeye.bench.BenchUtils
import popeye.pipeline.MetricGenerator
import popeye.proto.Message
import popeye.storage.hbase.HBaseStorage.{PointsGroups, ValueNameFilterCondition}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.Try

object PointsStorageBench {

  val timeRangeIdMapping = PeriodicGenerationId(PeriodicGenerationId.createPeriodConfigs(Seq(
    (
      1402862400, // 16/06/14
      168 // 1 week
      ),
    (
      1409515200, // 01/09/14
      24 // 1 day
      )
  )))

  val shardAttrs = Set("cluster")

  val tsdbFormat = new TsdbFormat(timeRangeIdMapping, shardAttrs)

  def main(args: Array[String]): Unit = {
    tsdbFormatBenchmark(
      numberOfPointsPerSeries = 1000,
      numberOfTagValues = (10, 10),
      timeStep = 300
    )
  }

  def tsdbFormatBenchmark(numberOfPointsPerSeries: Int,
                          numberOfTagValues: (Int, Int),
                          timeStep: Int): Unit = {
    val actorSystem = ActorSystem()
    try {
      implicit val exct = actorSystem.dispatcher
      val startTime = 1416395727 // Wed Nov 19 14:15:27 MSK 2014
      val metric = "test"
      val (numberOfHosts, numberOfDisks) = numberOfTagValues
      val hosts = (0 until numberOfHosts).map(i => f"w$i.qmon.yandex.net")
      val disks = (0 until numberOfDisks).map(i => f"sda$i")
      val timestamps = (0 until numberOfPointsPerSeries).map(i => startTime + i * timeStep)
      val points = createPoints(metric, Map("host" -> hosts, "disk" -> disks), timestamps)
      val qNames = points.flatMap(tsdbFormat.getAllQualifiedNames(_, timestamps.last)).toList.distinct
      val uniqueId = createUniqueId(actorSystem)
      val eventualIds = Future.sequence(qNames.map(name => uniqueId.resolveIdByName(name, create = true)(30 seconds)))
      Await.result(eventualIds, Duration.Inf)
      val keyValues = points.map {
        point =>
          val result = tsdbFormat.convertToKeyValue(point, uniqueId.findIdByName, timestamps.last)
          result.asInstanceOf[SuccessfulConversion].keyValue
      }
      val pointsTable = createHTablePool("tsdb").getTable("tsdb")
      val puts = keyValues.map {
        kv => new Put(kv.getRow).add(kv)
      }
      pointsTable.put(new util.ArrayList(puts.asJavaCollection))
      val results = pointsTable.getScanner(HBaseStorage.PointsFamily).asScala.toBuffer
      val benchResult = BenchUtils.bench(20, 10) {
        results.map(tsdbFormat.parseSingleValueRowResult)
      }
      println(f"number of points: ${ points.size }")
      println(f"number of tag values: $numberOfTagValues, points per series: $numberOfPointsPerSeries")
      println(f"time step: $timeStep")
      println(f"min time: ${ benchResult.minTime }, median time: ${ benchResult.medianTime }")
      println()
    } finally {
      actorSystem.shutdown()
      actorSystem.awaitTermination()
    }
  }

  def fakeHTableBenchmarks() {
    val system = ActorSystem()
    try {
      val storage = createStorage(system)
      benchmarkFakeHTable(storage, 100, 1000, 300)(system.dispatcher)
    } finally {
      system.shutdown()
      system.awaitTermination()
    }
  }

  def benchmarkFakeHTable(storage: HBaseStorage,
                          numberOfSeries: Int,
                          pointsPerSeries: Int,
                          timeStep: Int)
                         (implicit excon: ExecutionContext) = {
    val startTime = 1416395727 // Wed Nov 19 14:15:27 MSK 2014
    val timestamps = (0 until pointsPerSeries).map(i => startTime + i * timeStep)
    val metric = "test"
    val points = createPoints(metric, Map("tag" -> (0 until numberOfSeries).map(_.toString)), timestamps)
    while(Try(Await.result(storage.writePoints(points), Duration.Inf)).isFailure) {
      println("retrying write")
    }
    val timeRange = (timestamps.head, timestamps.last + 1)
    val benchResults = BenchUtils.bench(20000, 1) {
      val eventualPointsStream = storage.getPoints(
        metric,
        timeRange,
        Map("cluster" -> ValueNameFilterCondition.SingleValueName("test"))
      )
      val eventualPointsGroups = eventualPointsStream.flatMap(HBaseStorage.collectAllGroups)
      Await.result(eventualPointsGroups, Duration.Inf): PointsGroups
    }
    println(benchResults)
  }

  def createPoints(metric: String, tagValues: Map[String, Seq[String]], timestamps: Seq[Int]) = {
    for {
      tags <- MetricGenerator.generateTags(tagValues.toList)
      timestamp <- timestamps
    } yield {
      createPoint(metric, timestamp, tags.toMap)
    }
  }

  def createPoint(metric: String, timestamp: Int, tags: Map[String, String]) = {
    val attributes = (tags + (shardAttrs.head -> "test")).map {
      case (key, value) =>
        Message.Attribute.newBuilder()
          .setName(key).setValue(value)
          .build()
    }
    Message.Point.newBuilder()
      .setMetric("test")
      .setValueType(Message.Point.ValueType.INT)
      .setIntValue(0)
      .setTimestamp(timestamp)
      .addAllAttributes(attributes.asJava)
      .build()
  }

  def createStorage(actorSystem: ActorSystem) = {

    val metricRegistry = new MetricRegistry
    val pointsStorageMetrics = new HBaseStorageMetrics("hbase", metricRegistry)
    val id = new AtomicInteger(1)
    val pointsTableName = "tsdb"
    val hTablePool = createHTablePool(pointsTableName)
    def uniqueId = createUniqueId(actorSystem)
    new HBaseStorage(
      pointsTableName,
      hTablePool,
      uniqueId,
      tsdbFormat,
      pointsStorageMetrics,
      readChunkSize = 10
    )
  }

  def createUniqueId(actorSystem: ActorSystem) = {
    val metricRegistry = new MetricRegistry
    val uidTableName = "tsdb-uid"
    val uIdHTablePool = createHTablePool(uidTableName)

    def uniqActorProps = {
      val metrics = new UniqueIdStorageMetrics("uid", metricRegistry)
      val uniqueIdStorage = new UniqueIdStorage(uidTableName, uIdHTablePool, metrics)
      Props.apply(UniqueIdActor(uniqueIdStorage, actorSystem.dispatcher))
    }

    def uniqActor = actorSystem.actorOf(uniqActorProps)
    new UniqueIdImpl(uniqActor, new UniqueIdMetrics("uniqueid", metricRegistry))(actorSystem.dispatcher)
  }

  def createHTablePool(tableName: String): HTablePool = {
    val hTable = new FakeHTable(tableName, desc = null)
    new HTablePool(new Configuration(), 1, new HTableInterfaceFactory {
      def releaseHTableInterface(table: HTableInterface) {}

      def createHTableInterface(config: Configuration, tableNameBytes: Array[Byte]): HTableInterface = hTable
    })
  }

}
