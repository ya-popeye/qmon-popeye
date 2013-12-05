package popeye.query

import scala.collection.mutable


object PointAggregation {

  case class ActiveSeries(currentLineEnd: Int, series: BufferedIterator[Line])

  case class IdleSeries(firstLineStart: Int, series: BufferedIterator[Line])

  private implicit val activeSeriesOrdering = Ordering.by[ActiveSeries, Int](span => span.currentLineEnd).reverse
  private implicit val idleSeriesOrdering = Ordering.by[IdleSeries, Int](span => span.firstLineStart).reverse

  case class Line(x1: Int, y1: Double, x2: Int, y2: Double) {
    require(x1 <= x2)

    def next(xNext: Int, yNext: Double) = copy(x2, y2, xNext, yNext)

    def getY(x: Int) = {
      require(x1 <= x && x <= x2, f"x1 = $x1, x = $x, x2 = $x2")
      if (x == x1) y1
      else if (x == x2) y2
      else y1 + ((y2 - y1) * (x - x1)) / (x2 - x1)
    }
  }

  type Point = (Int, Double)
  type Graph = Iterator[Point]

  def linearInterpolation(spans: Seq[Graph], aggregateFunction: Seq[Double] => Double) = {
    val nonEmptySpans =
      spans
        .map(toLines)
        .filter(_.hasNext)
        .map(_.buffered)
    require(nonEmptySpans.nonEmpty)
    new AggregatingIterator(nonEmptySpans, aggregateFunction)
  }

  class AggregatingIterator(nonEmptySpans: Seq[BufferedIterator[Line]],
                            aggregateFunction: Seq[Double] => Double) extends Iterator[Point] {
    var idleSeries = {
      val series = nonEmptySpans.map(it => IdleSeries(it.head.x1, it))
      mutable.PriorityQueue(series: _*)
    }
    var activeSeries = mutable.PriorityQueue[ActiveSeries]()

    def hasNext: Boolean = activeSeries.nonEmpty || idleSeries.nonEmpty

    def next(): Point = {
      val currentX = nextX()
      if (idleSeries.nonEmpty) {
        activateIdleSeries(currentX)
      }
      val nextPoint = (currentX, currentValue(currentX))
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
      def nextIdleSpanStart = idleSeries.head.firstLineStart
      while(idleSeries.nonEmpty && nextIdleSpanStart == currentX) {
        val IdleSeries(_, series) = idleSeries.dequeue()
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
      val List((x1, y1), (x2, y2)) = firstTwoPoints
      val firstLine = Line(x1, y1, x2, y2)
      points.scanLeft(firstLine) {
        case (Line(_, _, lastX, lastY), (newX, newY)) => Line(lastX, lastY, newX, newY)
      }
    }
  }
}
