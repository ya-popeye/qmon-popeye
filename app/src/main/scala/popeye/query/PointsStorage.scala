package popeye.query

import org.apache.hadoop.hbase.util.Bytes
import popeye.PointRope
import popeye.storage.hbase.HBaseStorage.{PointsSeriesMap, PointsGroups}
import popeye.storage.ValueNameFilterCondition
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

  val aggregators = {
    import TsdbFormat.AggregationType._
    Map[AggregationType, Seq[Double] => Double](
      Sum -> (seq => seq.sum),
      Min -> (seq => seq.min),
      Max -> (seq => seq.max),
      Avg -> (seq => seq.sum / seq.size)
    )
  }

  val MaxGenerations = 3

  object NameType extends Enumeration {
    type NameType = Value
    val MetricType, AttributeNameType, AttributeValueType = Value
  }

  def createPointsStorage(pointsStorage: HBaseStorage,
                          uniqueIdStorage: UniqueIdStorage,
                          timeRangeIdMapping: GenerationIdMapping,
                          executionContext: ExecutionContext) = new PointsStorage {

    def getPoints(metric: String,
                  timeRange: (Int, Int),
                  attributes: Map[String, ValueNameFilterCondition],
                  downsampling: Option[(Int, TsdbFormat.AggregationType.AggregationType)],
                  cancellation: Future[Nothing]) = {
      implicit val exct = executionContext
      val eventualPointsSeriesMap = downsampling.map {
        downsampling =>
          getDownsampledTimeseriesFuture(metric, timeRange, attributes, downsampling, cancellation)
      }.getOrElse {
        getSeriesMapFuture(metric, timeRange, attributes, cancellation, NoDownsampling)
      }
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

    def getDownsampledTimeseriesFuture(metric: String,
                                       timeRange: (Int, Int),
                                       attributes: Map[String, ValueNameFilterCondition],
                                       downsampling: (Int, TsdbFormat.AggregationType.AggregationType),
                                       cancellation: Future[Nothing])
                                      (implicit extc: ExecutionContext): Future[PointsSeriesMap] = {
      val (downsampleInterval, aggregationType) = downsampling
      val closestResolutionOption = DownsamplingResolution.resolutions.to(downsampleInterval).lastOption.map(_._2)
      val storageDownsampling = closestResolutionOption.map {
        resolution => EnabledDownsampling(resolution, aggregationType)
      }.getOrElse {
        NoDownsampling
      }
      getDownsampledTimeseriesFutureRecursively(
        metric,
        timeRange,
        attributes,
        downsampleInterval,
        aggregationType,
        storageDownsampling,
        cancellation
      )
    }

    def getDownsampledTimeseriesFutureRecursively(metric: String,
                                                  timeRange: (Int, Int),
                                                  attributes: Map[String, ValueNameFilterCondition],
                                                  downsampleInterval: Int,
                                                  aggregationType: TsdbFormat.AggregationType.AggregationType,
                                                  storageDownsampling: Downsampling,
                                                  cancellation: Future[Nothing])
                                                 (implicit extc: ExecutionContext): Future[PointsSeriesMap] = {
      val (startTime, stopTime) = timeRange
      val seriesMapFuture = getSeriesMapFuture(metric, timeRange, attributes, cancellation, storageDownsampling)
      seriesMapFuture.flatMap {
        pointsSeriesMap =>
          val downsampledSeries = downsampleSeries(pointsSeriesMap, downsampleInterval, aggregators(aggregationType))
          val maxTimestamp = pointsSeriesMap.seriesMap.mapValues(_.last.timestamp).values.max
          val maxTimespanBoundary = {
            val baseTime = maxTimestamp - maxTimestamp % storageDownsampling.rowTimespanInSeconds
            baseTime + storageDownsampling.rowTimespanInSeconds
          }
          if (storageDownsampling != NoDownsampling && maxTimespanBoundary < stopTime) {
            val higherResolutionDs = getHigherResolution(storageDownsampling)
            val higherResolutionSeriesFuture = getDownsampledTimeseriesFutureRecursively(
              metric,
              (maxTimespanBoundary, stopTime),
              attributes,
              downsampleInterval,
              aggregationType,
              higherResolutionDs,
              cancellation
            )
            higherResolutionSeriesFuture.map {
              higherResolutionSeriesMap =>
                val downsampledHighResSeries =
                  downsampleSeries(pointsSeriesMap, downsampleInterval, aggregators(aggregationType))
                PointsSeriesMap.concat(downsampledSeries, downsampledHighResSeries)
            }
          } else {
            Future.successful(downsampledSeries)
          }
      }
    }

    def getHigherResolution(downsampling: Downsampling) = {
      val resolutionOption =
        DownsamplingResolution.resolutions.to(downsampling.resolutionInSeconds).init.lastOption.map(_._2)
      resolutionOption.map {
        resolution =>
          downsampling.asInstanceOf[EnabledDownsampling].copy(downsamplingResolution = resolution)
      }.getOrElse(NoDownsampling)
    }

    def downsampleSeries(pointsSeriesMap: PointsSeriesMap, interval: Int, aggregation: Seq[Double] => Double) = {
      val downsampledSeries = pointsSeriesMap.seriesMap.mapValues {
        series =>
          val downsampledIterator = PointSeriesUtils.downsample(series.iterator, interval, aggregation)
          PointRope.fromIterator(downsampledIterator)
      }.view.force
      PointsSeriesMap(downsampledSeries)
    }

    def getSeriesMapFuture(metric: String,
                           timeRange: (Int, Int),
                           attributes: Map[String, ValueNameFilterCondition],
                           cancellation: Future[Nothing],
                           storageDownsampling: Downsampling)
                          (implicit extc: ExecutionContext): Future[PointsSeriesMap] = {
      val groupsIterator = pointsStorage.getPoints(metric, timeRange, attributes, storageDownsampling)
      HBaseStorage.collectSeries(groupsIterator, cancellation)
    }

    def getSuggestions(namePrefix: String, nameType: NameType, maxSuggestions: Int): Seq[String] = {

      import NameType._
      val kind = nameType match {
        case MetricType => TsdbFormat.MetricKind
        case AttributeNameType => TsdbFormat.AttrNameKind
        case AttributeValueType => TsdbFormat.AttrValueKind
      }
      val currentTimeInSeconds = System.currentTimeMillis() / 1000
      val currentBaseTime = currentTimeInSeconds - currentTimeInSeconds % TsdbFormat.RAW_TIMESPAN
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


