package popeye.query

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import popeye.query.PointAggregation.{Point, Line}
import scala.collection.SortedMap
import scala.util.Random

class PointAggregationSpec extends FlatSpec with ShouldMatchers {

  behavior of "PointAggregation.toLines"

  it should "work" in {
    val input = (0 to 5).map(n => (n, 0.0))
    val lines = PointAggregation.toLines(input.iterator).toList
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

  behavior of "PointAggregation.linearInterpolation"

  it should "behave as no-op on single input" in {
    val input = (1 to 5).map(n => (n, n.toDouble))
    val aggregator: Seq[Double] => Double = {
      seq =>
        seq.size should be(1)
        seq.foldLeft(-1.0)((acc, x) => x)
    }
    val out = PointAggregation.linearInterpolation(Seq(input.iterator), aggregator)
    out.toList should equal(input.toList)
  }

  it should "behave as no-op on duplicated input" in {
    val input = (1 to 5).map(_ => (1 to 5).map(n => (n, n.toDouble)))
    val aggregator: Seq[Double] => Double = {
      seq =>
        seq.size should be(5)
        seq.foldLeft(-1.0)((acc, x) => x)
    }
    val out = PointAggregation.linearInterpolation(input.map(_.iterator), aggregator)
    out.toList should equal(input(0).toList)
  }

  it should "pass randomized test" in {
    val random = new Random(0)
    for (_ <- 1 to 100) {
      val numberOfInputs = 20
      val inputs = List.fill(numberOfInputs)(randomInput(random))
      val out = PointAggregation.linearInterpolation(inputs.map(_.iterator), maxAggregator).toList
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
    val outputIterator = PointAggregation.linearInterpolation(input, _.sum)
    val workTime = time {
      while(outputIterator.hasNext) {
        outputIterator.next()
      }
    }
    println(f"time in seconds = ${workTime * 0.001}")
  }

  def time[T](body: => Unit) = {
    val startTime = System.currentTimeMillis()
    body
    System.currentTimeMillis() - startTime
  }

  private def maxAggregator(seq: Seq[Double]): Double = seq.max

  private def randomInput(random: Random): Seq[Point] = {
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

  private def slowInterpolation(graphs: Seq[Seq[Point]], aggregationFunction: Seq[Double] => Double) = {
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
