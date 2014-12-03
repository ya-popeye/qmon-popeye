package popeye

import org.scalatest.{Matchers, FlatSpec}

class PointRopeSpec extends FlatSpec with Matchers {
  behavior of "PointArray"

  it should "fail on bad array sizing" in {
    intercept[IllegalArgumentException] {
      new PointArray(Array[Int](0, 1), Array[Double](0))
    }
  }

  it should "create valid iterators" in {
    val pArray = new PointArray(
      Array[Int](0, 1, 2),
      Array[Double](0, 10, 100)
    )
    val expected = List(
      Point(0, 0.0),
      Point(1, 10.0),
      Point(2, 100.0)
    )
    pArray.iterator.toList should equal(expected)
  }

  it should "create empty iterator" in {
    val pArray = new PointArray(
      Array[Int](),
      Array[Double]()
    )
    pArray.iterator.toList should be(empty)
  }

  it should "return correct size" in {
    val points = List(
      Point(0, 0.0),
      Point(1, 10.0),
      Point(2, 100.0)
    )
    val nonEmptyArray = PointArray.fromIterator(points.iterator)
    nonEmptyArray.size should equal(3)
    PointArray.fromIterator(Iterator()).size should equal(0)
  }

  behavior of "PointArray.fromIterator"

  it should "convert iterator to PointArray" in {
    val expected = List(
      Point(0, 0.0),
      Point(0, 10.0),
      Point(0, 100.0)
    )
    val pArray = PointArray.fromIterator(expected.iterator)
    pArray.iterator.toList should equal(expected)
  }

  it should "convert empty iterator to PointArray" in {
    val pArray = PointArray.fromIterator(Iterator.empty)
    pArray.iterator.toList should be(empty)
  }

  behavior of "PointRope.concatAll"
  it should "conatenate" in {
    val lists = List(
      List(Point(0, 0.0)),
      List(Point(0, 10.0), Point(1, 10.0)),
      List(Point(0, 100.0))
    )
    val pArrays = lists.map {
      points => PointRope.fromIterator(points.iterator)
    }
    val expected = lists.flatten
    PointRope.concatAll(pArrays).iterator.toList should equal(expected)
  }

  behavior of "PointRope.concat"

  it should "concatenate and return correct size" in {
    val pRopeLeft = PointRope(
      Point(0, 0.0)
    )
    val pRopeRight = PointRope(
      Point(1, 10.0),
      Point(2, 100.0)
    )
    val expected = List(
      Point(0, 0.0),
      Point(1, 10.0),
      Point(2, 100.0)
    )
    val result = pRopeLeft.concat(pRopeRight)
    result.iterator.toList should equal(expected)
    result.size should equal(3)
  }

  it should "return correct size" in {
    val points = List(
      Point(0, 0.0),
      Point(1, 10.0),
      Point(2, 100.0)
    )
    val pointRope = PointRope.fromIterator(points.iterator)
    pointRope.size should equal(3)
  }


}
