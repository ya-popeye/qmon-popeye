package popeye.query

import org.scalatest.FlatSpec
import popeye.query.PointSeriesUtils.{PlotPoint, Line}
import scala.util.Random
import org.scalatest.Matchers

class PointSeriesUtilsSpec extends FlatSpec with Matchers {

  behavior of "PointSeriesUtils.toLines"

  it should "work" in {
    val input = (0 to 5).map(n => (n, 0.0))
    val lines = PointSeriesUtils.toLines(input.iterator).toList
    def line(x1: Int, x2: Int) = Line(x1, 0.0, x2, 0.0)
    val expectedLines = List(
      line(0, 1),
      line(1, 2),
      line(2, 3),
      line(3, 4),
      line(4, 5)
    )
    lines should equal(expectedLines)
  }

  behavior of "PointSeriesUtils.interpolateAndAggregate"

  it should "behave as no-op on single input" in {
    val input = (1 to 5).map(n => (n, n.toDouble))
    val aggregator: Seq[Double] => Double = {
      seq =>
        seq.size should be(1)
        seq.foldLeft(-1.0)((acc, x) => x)
    }
    val out = PointSeriesUtils.interpolateAndAggregate(Seq(input.iterator), aggregator)
    out.toList should equal(input.toList)
  }

  it should "behave as no-op on duplicated input" in {
    val input = (1 to 5).map(_ => (1 to 5).map(n => (n, n.toDouble)))
    val aggregator: Seq[Double] => Double = {
      seq =>
        seq.size should be(5)
        seq.foldLeft(-1.0)((acc, x) => x)
    }
    val out = PointSeriesUtils.interpolateAndAggregate(input.map(_.iterator), aggregator)
    out.toList should equal(input(0).toList)
  }

  it should "pass randomized test" in {
    val random = new Random(0)
    for (_ <- 1 to 100) {
      val numberOfInputs = 20
      val inputs = List.fill(numberOfInputs)(randomInput(random))
      val out = PointSeriesUtils.interpolateAndAggregate(inputs.map(_.iterator), maxAggregator).toList
      val expectedOut = slowInterpolation(inputs, maxAggregator)
      if (out != expectedOut) {
        println(inputs)
        println(out)
        println(expectedOut)
      }
      out should equal(expectedOut)
    }
  }

  ignore should "have reasonable performance" in {
    def series = (0 to 1000000).iterator.map {
      i => (i, i.toDouble)
    }

    val input = (1 to 50).map(_ => series)
    val outputIterator = PointSeriesUtils.interpolateAndAggregate(input, _.sum)
    val workTime = time {
      while(outputIterator.hasNext) {
        outputIterator.next()
      }
    }
    println(f"time in seconds = ${workTime * 0.001}")
  }

  behavior of "PointSeriesUtils.downsample"

  it should "behave as no-op when interval == 1" in {
    val input = (0 to 5).map(i => (i, i.toDouble))
    val output = PointSeriesUtils.downsample(input.iterator, 1, maxAggregator).toList
    output should equal(input)
  }

  it should "handle simple case" in {
    val values = (0 until 5).flatMap {
      i =>
        val value = i.toDouble
        Seq.fill(5)(value)
    }
    val timestamps = 0 until 25
    val input = timestamps zip values
    val output = PointSeriesUtils.downsample(input.iterator, 5, maxAggregator).toList
    val expectedOutputValues = (0 until 5).map(i => i.toDouble)
    output.map(_._2) should equal(expectedOutputValues)
  }

  it should "pass randomized test" in {
    val random = new Random(0)
    val input = randomInput(random)
    val intervalLength = 1 + random.nextInt(10)
    val output = PointSeriesUtils.downsample(input.iterator, intervalLength, maxAggregator).toList
    val expectedOutput = slowDownsampling(input, intervalLength, maxAggregator)
    if (output != expectedOutput) {
      println(output)
      println(expectedOutput)
    }
    output should equal(expectedOutput)
  }

  ignore should "have reasonable performance" in {
    val series = (0 to 10000000).iterator.map {
      i => (i, i.toDouble)
    }

    val outputIterator = PointSeriesUtils.downsample(series, 100, maxAggregator)
    val workTime = time {
      while(outputIterator.hasNext) {
        outputIterator.next()
      }
    }
    println(f"time in seconds = ${workTime * 0.001}")
  }

  behavior of "PointSeriesUtils.differentiate"

  it should "not fail on empty source" in {
    PointSeriesUtils.differentiate(Iterator.empty).hasNext should be(false)
    PointSeriesUtils.differentiate(Iterator((1, 1.0))).hasNext should be(false)
  }

  it should "differentiate constant" in {
    val constPlot = (0 to 10).map(i => (i, 1.0))
    val div = PointSeriesUtils.differentiate(constPlot.iterator).toList
    div.map { case (x, y) => x} should equal(1 to 10)
    all(div.map { case (x, y) => math.abs(y)}) should (be < 0.000001)
  }

  it should "differentiate linear function" in {
    val constPlot = (0 to 10).map(i => (i, i.toDouble))
    val div = PointSeriesUtils.differentiate(constPlot.iterator).toList
    div.map { case (x, y) => x} should equal(1 to 10)
    all(div.map { case (x, y) => y}) should be(1.0 +- 0.000001)
  }

  private def time[T](body: => Unit) = {
    val startTime = System.currentTimeMillis()
    body
    System.currentTimeMillis() - startTime
  }

  private def slowDownsampling(source: Seq[PlotPoint],
                               intervalLength: Int,
                               aggregator: Seq[Double] => Double,
                               currentIntervalStartOption: Option[Int] = None): List[PlotPoint] = {
    if (source.isEmpty) return Nil
    val currentIntervalStart = currentIntervalStartOption.getOrElse(source.head._1)
    val (currentIntervalPoints, rest) = source.span(_._1 < (currentIntervalStart + intervalLength))
    val nextStart: Option[Int] = rest.headOption.map {
      head => currentIntervalStart + intervalLength * ((head._1 - currentIntervalStart) / intervalLength)
    }
    if (currentIntervalPoints.isEmpty) {
      slowDownsampling(rest, intervalLength, aggregator, nextStart)
    } else {
      val intervalTimestamp = currentIntervalStart + intervalLength / 2
      val intervalValue = aggregator(currentIntervalPoints.map(_._2))
      (intervalTimestamp, intervalValue) :: slowDownsampling(rest, intervalLength, aggregator, nextStart)
    }
  }

  private def maxAggregator(seq: Seq[Double]): Double = seq.max

  private def avgAggregator(seq: Seq[Double]): Double = seq.sum / seq.size

  private def randomInput(random: Random): Seq[PlotPoint] = {
    val inputSize = 2 + random.nextInt(50)
    val xs = {
      var xsSet = Set[Int]()
      while(xsSet.size != inputSize) {
        xsSet = xsSet + random.nextInt()
      }
      xsSet
    }
    val ys = List.fill(inputSize)(random.nextDouble())
    xs.toList.sorted zip ys
  }

  private def slowInterpolation(graphs: Seq[Seq[PlotPoint]], aggregationFunction: Seq[Double] => Double) = {
    val xs = {
      val allXs =
        for {
          graph <- graphs
          if graph.size > 1
          (x, _) <- graph
        } yield x
      allXs.distinct.sorted
    }
    xs.map {
      x =>
        val interpolations = graphs.map(interpolate(_, x)).filter(_.isDefined).map(_.get)
        (x, aggregationFunction(interpolations))
    }
  }

  private def interpolate(graph: Seq[(Int, Double)], x: Int): Option[Double] = {
    if (graph.size < 2) return None
    val closestLeftPoint = graph.takeWhile { case (graphX, _) => graphX <= x}.lastOption
    val closestRightPoint = graph.dropWhile { case (graphX, _) => graphX < x}.headOption
    for {
      (x1, y1) <- closestLeftPoint
      (x2, y2) <- closestRightPoint
    } yield Line(x1, y1, x2, y2).getY(x)
  }
}
