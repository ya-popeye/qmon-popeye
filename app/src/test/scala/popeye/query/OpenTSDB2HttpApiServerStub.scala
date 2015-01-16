package popeye.query

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import popeye.storage.hbase.TsdbFormat
import popeye.{PointRope, Point}
import popeye.query.PointsStorage.NameType
import popeye.query.PointsStorage.NameType.NameType
import popeye.storage.ValueNameFilterCondition.{MultipleValueNames, AllValueNames, SingleValueName}
import popeye.storage.ValueNameFilterCondition
import popeye.storage.hbase.HBaseStorage._

import scala.collection.immutable.SortedMap
import scala.concurrent.Future

object OpenTSDB2HttpApiServerStub {
  def main(args: Array[String]) {
    val system = ActorSystem()
    val metrics = Map(
      "sin" -> math.sin _,
      "cos" -> math.cos _
    )
    val periods = Seq(100, 500, 1000)
    val amps = Seq(1000, 2000, 4000)
    val adds = Seq(1000, 2000, 4000)
    case class Timeseries(metric: String, tags: SortedMap[String, String], f: Double => Double)
    val allTs =
      for {
        (metric, f) <- metrics
        period <- periods
        amp <- amps
        add <- adds
      } yield {
        val tags = SortedMap(
          "period" -> period.toString,
          "amp" -> amp.toString,
          "add" -> add.toString
        )
        val func: Double => Double = t => f(t / period * math.Pi) * amp + add
        Timeseries(metric, tags, func)
      }
    val storage = new PointsStorage {
      override def getPoints(metric: String,
                             timeRange: (Int, Int),
                             attributes: Map[String, ValueNameFilterCondition],
                             downsampling: Option[(Int, TsdbFormat.AggregationType.AggregationType)],
                             cancellation: Future[Nothing]): Future[PointsGroups] = {
        def conditionHolds(tagValue: String, condition: ValueNameFilterCondition) = condition match {
          case SingleValueName(name) => name == tagValue
          case MultipleValueNames(names) => names.contains(tagValue)
          case AllValueNames => true
        }
        val filteredTs = attributes.foldLeft(allTs.filter(_.metric == metric)) {
          case (nonFilteredTs, (tagKey, tagValueFilter)) =>
            nonFilteredTs
              .filter(_.tags.keySet.contains(tagKey))
              .filter(ts => conditionHolds(ts.tags(tagKey), tagValueFilter))
        }
        val (start, stop) = timeRange
        def createFunctionGraph(f: Double => Double) = {
          val points = (start to stop).by((stop - start) / 100).map(t => Point(t, f(t)))
          PointRope.fromIterator(points.iterator)
        }
        val pointSeries = filteredTs.map(ts => (ts.tags, createFunctionGraph(ts.f))).toMap
        val groupByTags = attributes.filter { case (tagKey, tagValueFilter) => tagValueFilter.isGroupByAttribute }.map(_._1)
        val groupedTs = pointSeries.groupBy {
          case (tags, _) =>
            val gTags = groupByTags.toList.map(tagK => (tagK, tags(tagK)))
            SortedMap(gTags: _*)
        }.mapValues(seriesMap => PointsSeriesMap(seriesMap))
        val groupsMap: Map[PointAttributes, PointsSeriesMap] = groupedTs
        Future.successful(PointsGroups(groupsMap))
      }

      override def getSuggestions(namePrefix: String,
                                  nameType: NameType,
                                  maxSuggestions: Int): Seq[String] = {
        def filterSuggestions(names: Iterable[String]) = names.filter(_.startsWith(namePrefix)).toList.sorted.distinct
        nameType match {
          case NameType.MetricType => filterSuggestions(metrics.keys)
          case NameType.AttributeNameType => filterSuggestions(Seq("period", "amp", "add"))
          case NameType.AttributeValueType => filterSuggestions(Seq(periods, amps, adds).flatten.map(_.toString))
        }
      }
    }
    val config = ConfigFactory.parseString(
      """
        |http = {
        |  backlog = 100
        |  listen = "localhost:8080"
        |}
      """.stripMargin)
    val serverMetrics = new OpenTSDB2HttpApiServerMetrics("otsdb.api", new MetricRegistry)
    new OpenTSDB2HttpApiServer(serverMetrics).runServer(config, storage, system, system.dispatcher)
  }
}
