package popeye.proto

import org.scalatest.{Matchers, FlatSpec}

class ExpandingBufferSpec extends FlatSpec with Matchers {
  behavior of "ExpandingBuffer"

  it should "consume bytes" in {
    val buffer = new ExpandingBuffer(2)
    buffer.write(Array[Byte](1, 2, 3))
    buffer.consume(1)
    buffer.toByteArray should equal(Array[Byte](2, 3))
  }
}
