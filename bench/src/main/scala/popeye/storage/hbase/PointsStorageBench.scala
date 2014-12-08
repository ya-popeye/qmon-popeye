package popeye.storage.hbase

import java.io.Closeable
import java.util
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Props}
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, HTableInterface, HTableInterfaceFactory, HTablePool}
import org.kiji.testing.fakehtable.FakeHTable
import popeye.Logging
import popeye.bench.BenchUtils
import popeye.pipeline.MetricGenerator
import popeye.proto.Message
import popeye.storage.hbase.HBaseStorage.{PointsGroups, ValueNameFilterCondition}
import popeye.util.hbase.HBaseConfigured

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import scala.util.Try

object PointsStorageBench extends Logging {

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

  val shardAttr = "cluster"
  val shardAttrValue = "test"

  val tsdbFormat = new TsdbFormat(timeRangeIdMapping, Set(shardAttr))

  def main(args: Array[String]): Unit = {
    args(0) match {
      case "convert" =>
        tsdbFormatBenchmark(
          numberOfPointsPerSeries = 1000,
          numberOfTagValues = (10, 10),
          timeStep = 300
        )
      case "load" =>
        hBaseBenchmark(args(1)).run()
    }
  }

  def hBaseBenchmark(zkQuorum: String,
                     numberOfPointsPerSeries: Int = 1000,
                     numberOfTagValues: (Int, Int) = (10, 10),
                     timeStep: Int = 300) = {
    val hBaseConfigured = new HBaseConfigured(ConfigFactory.empty(), zkQuorum)
    val hTablePool = hBaseConfigured.getHTablePool(2)
    for {
      actorSystem <- actorSystemResource
      hTablePool <- hTablePoolResource(hBaseConfigured)
    } yield {
      implicit val exct = actorSystem.dispatcher
      val uniqueId = createUniqueId(actorSystem, "tsdb-uid", hTablePool)
      val storageMetrics = new HBaseStorageMetrics("storage", new MetricRegistry)
      val storage = new HBaseStorage(
        "tsdb",
        hTablePool,
        uniqueId,
        tsdbFormat,
        storageMetrics,
        readChunkSize = 10
      )
      val currentTime = (System.currentTimeMillis() / 1000).toInt
      val metric = UUID.randomUUID().toString.replaceAll("-", "")
      val points = createTestPoints(currentTime, metric, numberOfPointsPerSeries, numberOfTagValues, timeStep)
      val (minTimestamp, maxTimestamp) = {
        val timestamps = points.map(_.getTimestamp)
        (timestamps.min.toInt, timestamps.max.toInt)
      }
      val eventialWrite = storage.writePoints(points)
      info("waiting for points to be written")
      Await.result(eventialWrite, Duration.Inf)
      info("write succeeded")
      val tags = Map(shardAttr -> HBaseStorage.ValueNameFilterCondition.SingleValueName(shardAttrValue))

      val benchResult = BenchUtils.bench(10, 1) {
        val pointsGroupsIterator = storage.getPoints(metric, (minTimestamp, maxTimestamp + 1), tags)
        val eventualPointsGroups = HBaseStorage.collectAllGroups(pointsGroupsIterator)
        Await.result(eventualPointsGroups, Duration.Inf)
      }

      println(s"number of points: ${ numberOfPointsPerSeries * numberOfTagValues._1 * numberOfTagValues._2 }")
      println(s"points per series: $numberOfPointsPerSeries, number if tag values: $numberOfTagValues")
      println(s"time step: $timeStep")
      println(s"min time ${ benchResult.minTime }, median time: ${ benchResult.medianTime }")
    }
  }

  def hTablePoolResource(hBaseConfigured: HBaseConfigured) =
    AFH.closableResource(hBaseConfigured.getHTablePool(2))

  def hTableResource(hTablePool: HTablePool, tableName: String) =
    AFH.closableResource(hTablePool.getTable(tableName))

  def tsdbFormatBenchmark(numberOfPointsPerSeries: Int,
                          numberOfTagValues: (Int, Int),
                          timeStep: Int): Unit = {
    val bench = for {
      actorSystem <- actorSystemResource
    }
    yield {
      implicit val exct = actorSystem.dispatcher
      val uniqueId = createUniqueId(actorSystem)
      val currentTime = 1416395727 // Wed Nov 19 14:15:27 MSK 2014
      val points = createTestPoints(currentTime, "test", numberOfPointsPerSeries, numberOfTagValues, timeStep)
      val qNames = points.flatMap(tsdbFormat.getAllQualifiedNames(_, currentTime)).toList.distinct
      val eventualIds = Future.sequence(qNames.map(name => uniqueId.resolveIdByName(name, create = true)(30 seconds)))
      Await.result(eventualIds, Duration.Inf)
      val keyValues = points.map {
        point =>
          val result = tsdbFormat.convertToKeyValue(point, uniqueId.findIdByName, currentTime)
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
      println(f"number of points: ${ keyValues.size }")
      println(f"number of tag values: $numberOfTagValues, points per series: $numberOfPointsPerSeries")
      println(f"time step: $timeStep")
      println(f"min time: ${ benchResult.minTime }, median time: ${ benchResult.medianTime }")
      println()
    }
    bench.run()
  }

  def actorSystemResource: AFH[ActorSystem] = {
    AFH.resource(ActorSystem()) {
      actorSystem =>
        info("stopping actor system")
        actorSystem.shutdown()
        actorSystem.awaitTermination()
        info("actor system terminated")
    }
  }

  def createTestPoints(currentTime: Int,
                       metric: String,
                       numberOfPointsPerSeries: Int,
                       numberOfTagValues: (Int, Int),
                       timeStep: Int): Seq[Message.Point] = {
    val startTime = currentTime - numberOfPointsPerSeries * timeStep
    val (numberOfHosts, numberOfDisks) = numberOfTagValues
    val hosts = (0 until numberOfHosts).map(i => f"w$i.qmon.yandex.net")
    val disks = (0 until numberOfDisks).map(i => f"sda$i")
    val timestamps = (0 until numberOfPointsPerSeries).map(i => startTime + i * timeStep)
    createPoints(metric, Map("host" -> hosts, "disk" -> disks), timestamps)
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
      val pointsGroupsIterator = storage.getPoints(
        metric,
        timeRange,
        Map("cluster" -> ValueNameFilterCondition.SingleValueName("test"))
      )
      val eventualPointsGroups = HBaseStorage.collectAllGroups(pointsGroupsIterator)
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
    val attributes = (tags + (shardAttr -> shardAttrValue)).map {
      case (key, value) =>
        Message.Attribute.newBuilder()
          .setName(key).setValue(value)
          .build()
    }
    Message.Point.newBuilder()
      .setMetric(metric)
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

  def createUniqueId(actorSystem: ActorSystem,
                     uidTableName: String = "tsdb-uid",
                     uIdHTablePool: HTablePool = createHTablePool("tsdb-uid")) = {
    val metricRegistry = new MetricRegistry

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

  // Automatic Failure Handling
  trait AFH[A] {
    def run(): A

    def compose[B](f: A => AFH[B]): AFH[B] = f(run())

    def map[B](f: A => B): AFH[B] = compose(x => AFH.lift(f(x)))

    def flatMap[B](f: A => AFH[B]): AFH[B] = compose(f)
  }

  trait AutoCloseAFH[A] extends AFH[A] {
    def close(x: A): Unit

    override def compose[B](f: (A) => AFH[B]): AFH[B] = {
      val self = this
      new AFH[B] {
        override def run(): B = {
          val resource = self.run()
          try {
            f(resource).run()
          } finally {
            close(resource)
          }
        }
      }
    }
  }


  object AFH {

    def lift[A](x: A): AFH[A] = new AFH[A] {
      override def run(): A = x
    }

    def closableResource[R <: Closeable](res: R): AFH[R] = resource(res)(_.close())

    def resource[R](resource: => R)(closeFunc: R => Unit): AFH[R] = new AutoCloseAFH[R] {
      override def run(): R = resource

      override def close(x: R): Unit = closeFunc(x)
    }
  }

}
