package popeye.query

import org.apache.hadoop.hbase.util.Bytes
import popeye.PointRope
import popeye.storage._
import popeye.storage.hbase.TsdbFormat.{EnabledDownsampling, DownsamplingResolution, Downsampling, NoDownsampling}
import scala.concurrent.{ExecutionContext, Future}
import popeye.storage.hbase._
import popeye.query.PointsStorage.NameType.NameType
import scala.collection.immutable.SortedSet
import scala.collection.immutable.SortedMap

trait PointsStorage {
  def getPoints(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition],
                downsampling: Option[(Int, TsdbFormat.AggregationType.AggregationType)],
                cancellation: Future[Nothing]): Future[PointsGroups]

  def getSuggestions(namePrefix: String, nameType: NameType, maxSuggestions: Int): Seq[String]
}

object PointsStorage {

  val MaxGenerations = 3

  object NameType extends Enumeration {
    type NameType = Value
    val MetricType, AttributeNameType, AttributeValueType = Value
  }

  def createPointsStorage(pointsStorage: TimeseriesStorage,
                          uniqueIdStorage: UniqueIdStorage,
                          timeRangeIdMapping: GenerationIdMapping,
                          executionContext: ExecutionContext) = new PointsStorage {

    val transparentDownsampling = new StorageWithTransparentDownsampling(pointsStorage)

    def getPoints(metric: String,
                  timeRange: (Int, Int),
                  attributes: Map[String, ValueNameFilterCondition],
                  downsampling: Option[(Int, TsdbFormat.AggregationType.AggregationType)],
                  cancellation: Future[Nothing]) = {
      implicit val exct = executionContext
      val eventualPointsSeriesMap = transparentDownsampling.getSeries(
        metric,
        timeRange,
        attributes,
        downsampling,
        cancellation
      )
      eventualPointsSeriesMap.map {
        seriesMap =>
          val groupByAttributeNames =
            attributes
              .toList
              .filter { case (attrName, valueFilter) => valueFilter.isGroupByAttribute}
              .map(_._1)
          val groups = seriesMap.seriesMap.groupBy {
            case (pointAttributes, _) =>
              val groupByAttributeValueIds = groupByAttributeNames.map(pointAttributes(_))
              SortedMap[String, String](groupByAttributeNames zip groupByAttributeValueIds: _*)
          }.mapValues(series => PointsSeriesMap(series))
          PointsGroups(groups)
      }
    }

    def getSuggestions(namePrefix: String, nameType: NameType, maxSuggestions: Int): Seq[String] = {

      import NameType._
      val kind = nameType match {
        case MetricType => TsdbFormat.MetricKind
        case AttributeNameType => TsdbFormat.AttrNameKind
        case AttributeValueType => TsdbFormat.AttrValueKind
      }
      val currentTimeInSeconds = System.currentTimeMillis() / 1000
      val currentBaseTime = currentTimeInSeconds - currentTimeInSeconds % TsdbFormat.MAX_TIMESPAN
      val generationIds = timeRangeIdMapping.backwardIterator(currentBaseTime.toInt)
        .take(MaxGenerations)
        .map(_.id)
        .toSeq
      val suggestions = generationIds.flatMap {
        genId => uniqueIdStorage.getSuggestions(
          kind,
          new BytesKey(Bytes.toBytes(genId)),
          namePrefix,
          maxSuggestions
        )
      }
      SortedSet(suggestions: _*).toSeq
    }
  }
}


