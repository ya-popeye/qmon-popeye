package popeye.storage

import popeye.PointRope
import popeye.query.PointSeriesUtils
import popeye.storage.hbase.TsdbFormat
import popeye.storage.hbase.TsdbFormat.{Downsampling, EnabledDownsampling, DownsamplingResolution, NoDownsampling}
import StorageWithTransparentDownsampling._

import scala.concurrent.{ExecutionContext, Future}

object StorageWithTransparentDownsampling{
  val aggregators = {
    import TsdbFormat.AggregationType._
    Map[AggregationType, Seq[Double] => Double](
      Sum -> (seq => seq.sum),
      Min -> (seq => seq.min),
      Max -> (seq => seq.max),
      Avg -> (seq => seq.sum / seq.size)
    )
  }

}

class StorageWithTransparentDownsampling(timeseriesStorage: TimeseriesStorage) {

  def getSeries(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition],
                downsampling: Option[(Int, TsdbFormat.AggregationType.AggregationType)],
                cancellation: Future[Nothing])
               (implicit extc: ExecutionContext): Future[PointsSeriesMap] = {
    downsampling.map {
      downsampling =>
        getDownsampledTimeseriesFuture(metric, timeRange, attributes, downsampling, cancellation)
    }.getOrElse {
      timeseriesStorage.getSeries(metric, timeRange, attributes, NoDownsampling, cancellation)
    }
  }

  private def getDownsampledTimeseriesFuture(metric: String,
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

  private def getDownsampledTimeseriesFutureRecursively(metric: String,
                                                timeRange: (Int, Int),
                                                attributes: Map[String, ValueNameFilterCondition],
                                                downsampleInterval: Int,
                                                aggregationType: TsdbFormat.AggregationType.AggregationType,
                                                storageDownsampling: Downsampling,
                                                cancellation: Future[Nothing])
                                               (implicit extc: ExecutionContext): Future[PointsSeriesMap] = {
    val (startTime, stopTime) = timeRange
    val seriesMapFuture = timeseriesStorage.getSeries(metric, timeRange, attributes, storageDownsampling, cancellation)
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

  private def getHigherResolution(downsampling: Downsampling) = {
    val resolutionOption =
      DownsamplingResolution.resolutions.to(downsampling.resolutionInSeconds).init.lastOption.map(_._2)
    resolutionOption.map {
      resolution =>
        downsampling.asInstanceOf[EnabledDownsampling].copy(downsamplingResolution = resolution)
    }.getOrElse(NoDownsampling)
  }

  private def downsampleSeries(pointsSeriesMap: PointsSeriesMap, interval: Int, aggregation: Seq[Double] => Double) = {
    val downsampledSeries = pointsSeriesMap.seriesMap.mapValues {
      series =>
        val downsampledIterator = PointSeriesUtils.downsample(series.iterator, interval, aggregation)
        PointRope.fromIterator(downsampledIterator)
    }.view.force
    PointsSeriesMap(downsampledSeries)
  }
}
