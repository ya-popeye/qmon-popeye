package popeye.util

import org.scalatest.{Matchers, FlatSpec}

class CollectionUtilsSpec extends FlatSpec with Matchers {
  behavior of "CollectionUtils.uniqIterable"
  it should "return same elements" in {
    CollectionsUtils.uniqIterable[Int](Seq(1, 2, 3, 4), _ == _).toList should equal(Seq(1, 2, 3, 4))
  }

  it should "filter repetitive elements" in {
    CollectionsUtils.uniqIterable[Int](Seq(1, 1, 2, 3, 3, 4), _ == _).toList should equal(Seq(1, 2, 3, 4))
  }

  it should "filter all but first element" in {
    CollectionsUtils.uniqIterable[Int](Seq(1, 1, 1), _ == _).toList should equal(Seq(1))
  }

  it should "handle empty case" in {
    CollectionsUtils.uniqIterable[Int](Seq(), _ == _).toList should equal(Seq())
  }
}
