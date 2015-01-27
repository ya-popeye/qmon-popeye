package popeye.storage

import popeye.{AsyncIterator, Logging, PointRope}
import popeye.query.PointSeriesUtils
import popeye.storage.hbase.{HBaseStorage, TsdbFormat}
import popeye.storage.hbase.TsdbFormat._
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

class StorageWithTransparentDownsampling(timeseriesStorage: TimeseriesStorage) extends Logging {

  def getSeries(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition],
                downsampling: Option[(Int, TsdbFormat.AggregationType.AggregationType)],
                cancellation: Future[Nothing])
               (implicit extc: ExecutionContext): Future[PointsSeriesMap] = {
    downsampling.map {
      downsampling =>
        info("using predownsampled data")
        getDownsampledTimeseriesFuture(metric, timeRange, attributes, downsampling, cancellation)
    }.getOrElse {
      info("downsampling is off")
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
    info(f"choosing closest resolution: $closestResolutionOption")
    val storageDownsampling = closestResolutionOption.map {
      resolution => EnabledDownsampling(resolution, aggregationType)
    }.getOrElse {
      NoDownsampling
    }
    val allRequiredResolutions = fetchRequiredResolutions(
      metric,
      timeRange,
      attributes,
      cancellation,
      storageDownsampling
    )
    val downsampledPoints = allRequiredResolutions.map {
      pointSeriesMap =>
        Future {
          downsampleSeries(pointSeriesMap, downsampleInterval, aggregators(aggregationType))
        }
    }
    HBaseStorage.collectSeries(downsampledPoints, cancellation)
  }

  private def fetchRequiredResolutions(metric: String,
                                       timeRange: (Int, Int),
                                       attributes: Map[String, ValueNameFilterCondition],
                                       cancellation: Future[Nothing],
                                       startingDownsampling: Downsampling)
                                      (implicit eCtx: ExecutionContext): AsyncIterator[PointsSeriesMap] = {
    val (startTime, stopTime) = timeRange
    def fetchPoints(downsampling: Downsampling, timeRange: (Int, Int)): Future[(Downsampling, PointsSeriesMap)] = {
      timeseriesStorage.getSeries(metric, timeRange, attributes, downsampling, cancellation).map {
        pointSeriesMap => (downsampling, pointSeriesMap)
      }
    }
    def fetchNextPointsIfNeeded(currentPoints: (Downsampling, PointsSeriesMap)) = {
      val (currentDownsampling, pointsSeriesMap) = currentPoints
      val maxTimestamp = pointsSeriesMap.seriesMap.mapValues(_.last.timestamp).values.max
      val maxTimespanBoundary = {
        val baseTime = maxTimestamp - maxTimestamp % currentDownsampling.resolutionInSeconds
        baseTime + currentDownsampling.resolutionInSeconds
      }
      info(f"fetched series has timestamps up to $maxTimestamp, max boundary: $maxTimespanBoundary")
      if (currentDownsampling != NoDownsampling && maxTimespanBoundary < stopTime) {
        val higherResolutionDs = getHigherResolution(currentDownsampling)
        val future = fetchPoints(higherResolutionDs, (maxTimespanBoundary, stopTime))
        Some(future)
      } else {
        None
      }
    }
    val initialFetch = fetchPoints(startingDownsampling, timeRange)
    AsyncIterator.iterate(initialFetch)(fetchNextPointsIfNeeded).map {
      case (downsampling, points) => Future.successful(points)
    }
  }

  private def getHigherResolution(downsampling: Downsampling) = {
    val higherResolutions = DownsamplingResolution.resolutions.to(downsampling.resolutionInSeconds - 1).map(_._2)
    val resolutionOption = higherResolutions.lastOption
    resolutionOption.map {
      resolution => downsampling.asInstanceOf[EnabledDownsampling].copy(downsamplingResolution = resolution)
    }.getOrElse {
      NoDownsampling
    }
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
