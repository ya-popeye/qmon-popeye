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
    case class FetchRequest(startTime: Int, downsampling: Downsampling)
    def fetchPoints(request: FetchRequest): Future[(FetchRequest, PointsSeriesMap)] = {
      info(f"fetching: $request")
      val pointSeriesMapFuture = timeseriesStorage.getSeries(
        metric,
        (request.startTime, stopTime),
        attributes,
        request.downsampling,
        cancellation
      )
      pointSeriesMapFuture.map {
        pointSeriesMap => (request, pointSeriesMap)
      }
    }
    def fetchNextPointsIfNeeded(currentPoints: (FetchRequest, PointsSeriesMap)) = {
      val (previousRequest, pointsSeriesMap) = currentPoints
      val previousDownsampling = previousRequest.downsampling
      if (previousDownsampling == NoDownsampling) {
        None
      } else {
        val latestTimestampOption = getLatestTimestamp(pointsSeriesMap)
        val maxResolutionBoundaryOption = latestTimestampOption.map {
          latestTimestamp =>
            val baseTime = latestTimestamp - latestTimestamp % previousDownsampling.resolutionInSeconds
            baseTime + previousDownsampling.resolutionInSeconds
        }
        info(f"fetched series has timestamps up to $latestTimestampOption, max boundary: $maxResolutionBoundaryOption")
        val isMoreDataRequired = maxResolutionBoundaryOption.map(_ < stopTime).getOrElse(true)
        if (isMoreDataRequired) {
          val higherResolutionDs = getHigherResolution(previousDownsampling)
          val newStartTime = maxResolutionBoundaryOption.getOrElse(previousRequest.startTime)
          val future = fetchPoints(FetchRequest(newStartTime, higherResolutionDs))
          Some(future)
        } else {
          None
        }
      }
    }
    val initialFetch = fetchPoints(FetchRequest(startTime, startingDownsampling))
    AsyncIterator.iterate(initialFetch)(fetchNextPointsIfNeeded).map {
      case (request, points) => Future.successful(points)
    }
  }

  private def getLatestTimestamp(pointsSeriesMap: PointsSeriesMap): Option[Int] = {
    val lastTimestamps = pointsSeriesMap.seriesMap.values.map {
      rope => rope.lastOption.map(_.timestamp)
    }.collect {
      case Some(ts) => ts
    }
    if (lastTimestamps.nonEmpty) {
      Some(lastTimestamps.max)
    } else {
      None
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
