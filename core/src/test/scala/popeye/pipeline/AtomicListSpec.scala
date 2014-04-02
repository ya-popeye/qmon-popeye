package popeye.pipeline

import org.scalatest.FlatSpec

class AtomicListSpec extends FlatSpec {
  behavior of "AtomicList"

  it should "not hang on headOption of empty list" in {
    val list = new AtomicList()
    list.headOption
  }
}
