package popeye.packing

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Scan, HBaseAdmin}
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import popeye.{Point, Logging}
import popeye.inttesting.PopeyeIntTestingUtils
import popeye.paking.TsdbRegionObserver
import popeye.storage.ValueNameFilterCondition.SingleValueName
import popeye.storage.hbase._
import popeye.util.ZkConnect
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import scala.concurrent.Await

import scala.concurrent.duration._

class PackingIntSpec extends FlatSpec with Matchers with BeforeAndAfter with Logging {

  behavior of "TsdbRegionObserver"

  var hbaseTestingUtility: HBaseTestingUtility = null
  var hbaseZkConnect: ZkConnect = null
  var actorSystem: ActorSystem = null

  before {
    hbaseTestingUtility = HBaseTestingUtility.createLocalHTU()
    info("creating zookeeper minicluster")
    val miniZkCluster = hbaseTestingUtility.startMiniZKCluster()
    info("creating hbase minicluster")
    hbaseTestingUtility.startMiniCluster()
    hbaseZkConnect = ZkConnect(Seq("localhost" -> Some(miniZkCluster.getClientPort)), None)
    actorSystem = ActorSystem()
  }

  after {
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    hbaseTestingUtility.shutdownMiniCluster()
  }

  it should "pack rows" in {
    implicit val exct = actorSystem.dispatcher
    val pointsTableName = "tsdb"
    val uidTableName = "tsdb-uid"
    createTables(hbaseTestingUtility.getConfiguration, pointsTableName, uidTableName)
    val tsdbFormatConfig = TsdbFormatConfig(Seq(StartTimeAndPeriod("15/01/05", 336)), Set("cluster"))
    val hbaseStorageConfig = HBaseStorageConfig(
      hbaseConfig = ConfigFactory.empty(),
      hbaseTestingUtility.getConfiguration,
      uidsTableName = uidTableName,
      pointsTableName = pointsTableName,
      pointsTableCoprocessorJarPathOption = None,
      poolSize = 10,
      zkQuorum = hbaseZkConnect,
      readChunkSize = 100,
      resolveTimeout = 10 seconds,
      uidsCacheInitialCapacity = 10000,
      uidsCacheMaxCapacity = 10000,
      tsdbFormatConfig = tsdbFormatConfig,
      storageName = "hbase"
    )
    val metricRegistry = new MetricRegistry()
    val hbaseStorage = new HBaseStorageConfigured(hbaseStorageConfig, actorSystem, metricRegistry)
    val stopTime = {
      val time = (System.currentTimeMillis() / 1000).toInt
      time - time % 3600
    }
    val timestamps = (stopTime - 3600) until stopTime
    val points = timestamps.map {
      ts => PopeyeIntTestingUtils.createPoint("test", ts, Seq("cluster" -> "test"), Left(ts))
    }
    val (firstPoints, secondPoints) = points.splitAt(1800)
    Await.result(hbaseStorage.storage.writePoints(firstPoints), Duration.Inf)
    hbaseTestingUtility.flush(TableName.valueOf(pointsTableName))
    Await.result(hbaseStorage.storage.writePoints(secondPoints), Duration.Inf)
    hbaseTestingUtility.flush(TableName.valueOf(pointsTableName))
    hbaseTestingUtility.compact(TableName.valueOf(pointsTableName), false)
    val pointsTable = hbaseStorage.hTablePool.getTable(pointsTableName)
    val scanner = pointsTable.getScanner(new Scan)
    val results = scanner.asScala.toList
    scanner.close()
    pointsTable.close()
    val seriesIterator = hbaseStorage.storage.getPoints(
      "test",
      (stopTime - 3600, stopTime),
      Map("cluster" -> SingleValueName("test"))
    )
    val pointsSeriesMap = Await.result(HBaseStorage.collectSeries(seriesIterator), Duration.Inf)
    results.size should equal(1)
    val expectedPoints = points.map(point => Point(point.getTimestamp.toInt, point.getTimestamp))
    pointsSeriesMap.seriesMap(SortedMap("cluster" -> "test")).iterator.toList should equal(expectedPoints)
  }


  def createTables(hbaseConfiguration: Configuration, pointsTableName: String, uidsTableName: String) {
    def getNamespace(tableName: String) = TableName.valueOf(tableName).getNamespaceAsString

    val namespaces = Seq(pointsTableName, uidsTableName).map(getNamespace)

    val tsdbTable = {
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(pointsTableName))
      val tsdbColumn = new HColumnDescriptor("t")
      tsdbColumn.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      tableDescriptor.addFamily(tsdbColumn)
      tableDescriptor.addCoprocessor(classOf[TsdbRegionObserver].getCanonicalName)
      tableDescriptor
    }

    val tsdbUidTable = {
      val nameColumn = new HColumnDescriptor("name")
      val idColumn = new HColumnDescriptor("id")
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(uidsTableName))
      tableDescriptor.addFamily(nameColumn)
      tableDescriptor.addFamily(idColumn)
      tableDescriptor
    }

    val hBaseAdmin = new HBaseAdmin(hbaseConfiguration)
    try {
      for (namespace <- namespaces) {
        try {
          info(f"creating namespace $namespace")
          hBaseAdmin.createNamespace(NamespaceDescriptor.create(namespace).build())
          info(f"namespace $namespace was created")
        } catch {
          case e: NamespaceExistException => // do nothing
        }
      }
      try {
        info(f"creating points table $tsdbTable")
        hBaseAdmin.createTable(tsdbTable)
        info(f"points table $tsdbTable was created")
      } catch {
        case e: TableExistsException => // do nothing
      }
      try {
        info(f"creating uids table $tsdbUidTable")
        hBaseAdmin.createTable(tsdbUidTable)
        info(f"uids table $tsdbUidTable was created")
      } catch {
        case e: TableExistsException => // do nothing
      }
    } finally {
      hBaseAdmin.close()
    }
  }
}
