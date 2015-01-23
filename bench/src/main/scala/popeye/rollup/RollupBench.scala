package popeye.rollup

import org.apache.hadoop.hbase.Cell
import popeye.bench.BenchUtils
import popeye.clients.TsPoint
import popeye.inttesting.TestDataUtils
import popeye.proto.Message
import popeye.rollup.RollupMapperEngine.RollupStrategy
import popeye.storage.hbase.TsdbFormat.NoDownsampling
import popeye.storage.hbase._
import scala.collection.immutable
import scala.collection.JavaConverters._

import org.apache.hadoop.hbase.client.Result

import scala.collection.SortedMap
import scala.util.Random

object RollupBench {
  val shardAttributeName = "cluster"

  def main(args: Array[String]) {
    val tsdbFormat =
      TsdbFormatConfig(Seq(StartTimeAndPeriod("05/11/14", 26 /*one year*/)), Set(shardAttributeName)).tsdbFormat
    val engine = new RollupMapperEngine(tsdbFormat, RollupStrategy.HourRollup, 1421280000)
    val results = createResults(tsdbFormat)
    println(f"total results count: ${results.size}, total points count: ${results.size * 60}")
    val benchResult = BenchUtils.bench(10, 10) {
      for (result <- results) {
        engine.map(result).asScala.toBuffer
      }
    }

    println(benchResult)

  }

  def createResults(tsdbFormat: TsdbFormat) = {
    val currentTimeSeconds = 1421280000
    val periods = Seq(Array[Byte](0, 0, 1), Array[Byte](0, 0, 3), Array[Byte](0, 0, 2))
    val shifts = Seq(Array[Byte](0, 1, 1), Array[Byte](0, 1, 3), Array[Byte](0, 1, 2))
    val amps = Seq(Array[Byte](0, 2, 1), Array[Byte](0, 2, 3), Array[Byte](0, 2, 2))
    val periodAttrName = Array[Byte](0, 0, 1)
    val shiftAttrName = Array[Byte](0, 0, 2)
    val ampAttrName = Array[Byte](0, 0, 3)
    val shard = Array[Byte](0, 0, 3)
    val metric = Array[Byte](0, 0, 3)

    val firstTimestamp = currentTimeSeconds - 3600 * 24 * 7
    val baseTimes = firstTimestamp to currentTimeSeconds by 3600
    val random = new Random(0)
    for {
      baseTime <- baseTimes
      period <- periods
      shift <- shifts
      amp <- amps
    } yield {
      val timeseriesId = TimeseriesId(
        generationId = new BytesKey(Array[Byte](0, 1)),
        downsampling = NoDownsampling,
        metricId = new BytesKey(metric),
        valueTypeId = TsdbFormat.ValueTypes.SingleValueTypeStructureId,
        shardId = shard,
        attributeIds = immutable.SortedMap(
          new BytesKey(periodAttrName) -> new BytesKey(period),
          new BytesKey(ampAttrName) -> new BytesKey(amp),
          new BytesKey(shiftAttrName) -> new BytesKey(shift)
        )
      )

      val keyValues = for (ts <- baseTime until (baseTime + 3600) by 60) yield {
        tsdbFormat.createPointKeyValue(
          timeseriesId,
          ts,
          Left(random.nextLong()),
          currentTimeSeconds
        ).asInstanceOf[Cell]
      }
      Result.create(keyValues.asJava)
    }
  }
}
