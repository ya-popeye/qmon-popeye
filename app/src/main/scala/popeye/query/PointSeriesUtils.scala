package popeye.query

import popeye.Point

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object PointSeriesUtils {

  case class ActiveSeries(currentLineEnd: Int, series: BufferedIterator[Line])

  case class IdleSeries(firstLineStart: Int, series: BufferedIterator[Line])

  case class Line(x1: Int, y1: Double, x2: Int, y2: Double) {
    require(x1 <= x2, f"$x1 > $x2")

    def next(xNext: Int, yNext: Double) = copy(x2, y2, xNext, yNext)

    def getY(x: Int) = {
      require(x1 <= x && x <= x2, f"x1 = $x1, x = $x, x2 = $x2")
      if (x == x1) y1
      else if (x == x2) y2
      else y1 + ((y2 - y1) * (x - x1)) / (x2 - x1)
    }
  }

  def interpolateAndAggregate(series: Seq[Iterator[Point]], aggregateFunction: Seq[Double] => Double): Iterator[Point] = {
    val nonEmptySeries =
      series
        .map(toLines)
        .filter(_.hasNext)
        .map(_.buffered)
    require(series.nonEmpty, "input is empty")
    require(nonEmptySeries.nonEmpty, "all series are empty")
    new AggregatingIterator(nonEmptySeries, aggregateFunction)
  }

  class AggregatingIterator(nonEmptySpans: Seq[BufferedIterator[Line]],
                            aggregateFunction: Seq[Double] => Double) extends Iterator[Point] {
    var idleSeries = {
      val series = nonEmptySpans.map(it => IdleSeries(it.head.x1, it))
      series.sortBy(_.firstLineStart)
    }
    var activeSeries = mutable.PriorityQueue[ActiveSeries]()(new Ordering[ActiveSeries] {
      def compare(x: ActiveSeries, y: ActiveSeries): Int = {
        // java.lang.Integer.compare is used to avoid autoboxing
        Integer.compare(y.currentLineEnd, x.currentLineEnd)
      }
    })

    def hasNext: Boolean = activeSeries.nonEmpty || idleSeries.nonEmpty

    def next(): Point = {
      val currentX = nextX()
      if (idleSeries.nonEmpty) {
        activateIdleSeries(currentX)
      }
      val nextPoint = Point(currentX, currentValue(currentX))
      updateActiveSeries(currentX)
      nextPoint
    }

    private def nextX() = {
      if (activeSeries.nonEmpty && idleSeries.nonEmpty) {
        val closestActiveEnd = activeSeries.head.currentLineEnd
        val closestIdleStart = idleSeries.head.firstLineStart
        math.min(closestActiveEnd, closestIdleStart)
      } else if (activeSeries.nonEmpty) {
        activeSeries.head.currentLineEnd
      } else {
        idleSeries.head.firstLineStart
      }
    }

    private def updateActiveSeries(currentX: Int) {
      require(activeSeries.head.currentLineEnd >= currentX)
      while(activeSeries.nonEmpty && activeSeries.head.currentLineEnd == currentX) {
        val series = activeSeries.dequeue().series
        if (series.hasNext) {
          series.next()
        }
        // double check is required because BufferedIterator.next calls Iterator.next() internally
        if (series.hasNext) {
          val x = series.head.x2
          activeSeries.enqueue(ActiveSeries(x, series))
        }
      }
    }

    private def currentValue(currentX: Int) = {
      import scala.collection.breakOut
      val values = activeSeries.map(activeSeries => activeSeries.series.head.getY(currentX))(breakOut)
      aggregateFunction(values)
    }

    private def activateIdleSeries(currentX: Int) {
      val seriesToActivate = idleSeries.takeWhile(_.firstLineStart == currentX)
      idleSeries = idleSeries.dropWhile(_.firstLineStart == currentX)
      for (IdleSeries(_, series) <- seriesToActivate) {
        val line = series.head
        activeSeries.enqueue(ActiveSeries(line.x2, series))
      }
    }

  }

  def toLines(points: Iterator[Point]): Iterator[Line] = {
    val firstTwoPoints = points.take(2).toList
    if (firstTwoPoints.size != 2) {
      Iterator.empty
    } else {
      val List(Point(x1, y1), Point(x2, y2)) = firstTwoPoints
      val firstLine = Line(x1, y1, x2, y2)
      points.scanLeft(firstLine) {
        case (Line(_, _, lastX, lastY), Point(newX, newY)) => Line(lastX, lastY, newX, newY)
      }
    }
  }

  def interpolateAndDownsample(graphPointIterators: Seq[Iterator[Point]],
                               interpolationAggregator: (Seq[Double]) => Double,
                               downsamplingInterval: Option[Int]): Iterator[Point] = {
    val aggregated = interpolateAndAggregate(graphPointIterators, interpolationAggregator)
    downsamplingInterval.map {
      interval =>
        val average: (Seq[Double]) => Double = seq => seq.sum / seq.size
        PointSeriesUtils.downsample(aggregated, interval, average)
    }.getOrElse {
      aggregated
    }
  }

  def downsample(source: Iterator[Point],
                 intervalLength: Int,
                 aggregator: Seq[Double] => Double): Iterator[Point] = {
    new DownsamplingIterator(source, intervalLength, aggregator)
  }

  class DownsamplingIterator(source: Iterator[Point],
                             intervalLength: Int,
                             aggregator: Seq[Double] => Double) extends Iterator[Point] {
    var currentIntervalStart = 0
    val buffer = {
      val buf = ArrayBuffer[Double]()
      if (source.hasNext) {
        val Point(firstTimestamp, firstValue) = source.next()
        currentIntervalStart = firstTimestamp - firstTimestamp % intervalLength
        buf += firstValue
      }
      buf
    }

    def hasNext: Boolean = buffer.nonEmpty

    def next(): Point = {
      if (!source.hasNext) {
        val point = Point(currentIntervalStart + intervalLength / 2, aggregator(buffer))
        buffer.clear()
        return point
      }
      var nextPoint = source.next()
      val nextIntervalStart = currentIntervalStart + intervalLength
      while(source.hasNext && nextPoint.timestamp < nextIntervalStart) {
        buffer += nextPoint.value
        nextPoint = source.next()
      }
      val aggregatedValue =
        if (source.hasNext || nextPoint.timestamp >= nextIntervalStart) {
          val value = aggregator(buffer)
          buffer.clear()
          buffer += nextPoint.value
          value
        }
        else {
          buffer += nextPoint.value
          val value = aggregator(buffer)
          buffer.clear()
          value
        }
      val intervalTime = currentIntervalStart + intervalLength / 2
      currentIntervalStart = nextPoint.timestamp - nextPoint.timestamp % intervalLength

      Point(intervalTime, aggregatedValue)
    }

  }

  def differentiate(source: Iterator[Point]): Iterator[Point] = {
    if (source.hasNext) {
      new DifferentiatingIterator(source, source.next())
    } else {
      Iterator.empty
    }
  }

  class DifferentiatingIterator(source: Iterator[Point], startPoint: Point) extends Iterator[Point] {

    var lastPoint = startPoint

    def hasNext: Boolean = source.hasNext

    def next(): Point = {
      val Point(lastX, lastY) = lastPoint
      val currentPoint@Point(currentX, currentY) = source.next()
      lastPoint = currentPoint
      val y = (currentY - lastY) / (currentX - lastX)
      Point(currentX, y)
    }
  }

}
