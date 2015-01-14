package popeye.proto

import org.scalatest.{Matchers, FlatSpec}

class ExpandingArraySpec extends FlatSpec with Matchers {
  behavior of "ExpandingArray"

  it should "add elements from array" in {
    val arr = new ExpandingArray[Int](2)
    val elements = Array(0, 0, 1, 2)
    arr.add(elements, 2, 2)
    arr.buffer should equal(Array(1, 2))
  }
}
