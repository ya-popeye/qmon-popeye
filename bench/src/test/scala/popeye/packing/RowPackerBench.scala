package popeye.packing

import akka.actor.ActorSystem
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Scan
import popeye.bench.BenchUtils
import popeye.inttesting.PopeyeIntTestingUtils
import popeye.storage.hbase.{TsdbFormat, PointsStorageStub}
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._

object RowPackerBench {

  def createKeyValues(nKeyValues: Int): Seq[Seq[Cell]] = {
    val points = (0 until nKeyValues).map {
      ts => PopeyeIntTestingUtils.createPoint("test", ts, Seq("cluster" -> "test"), Left(ts))
    }
    implicit val actorSystem = ActorSystem()
    implicit val exct = actorSystem.dispatcher

    val storage = new PointsStorageStub(shardAttrs = Set("cluster"))
    Await.result(storage.storage.writePoints(points), Duration.Inf)
    val results = storage.pointsTable.getScanner(new Scan()).asScala.toList.map(_.rawCells().toSeq)
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    results
  }

  def main(args: Array[String]) {
    val rowPacker = TsdbFormat.rowPacker
    val keyValues = createKeyValues(100000)
    // 1 million points per sample
    val benchResult = BenchUtils.bench(100, 10) {
      for (row <- keyValues) {
        rowPacker.packRow(row)
      }
    }
    println(benchResult)
  }
}
