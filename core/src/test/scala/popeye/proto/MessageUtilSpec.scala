package popeye.proto

import org.scalatest.{Matchers, FlatSpec}
import popeye.test.PopeyeTestUtils

class MessageUtilSpec extends FlatSpec with Matchers {
  behavior of "MessageUtil.validatePoint"
  it should "pass list point" in {
    val listPoint = PopeyeTestUtils.createListPoint()
    MessageUtil.validatePoint(listPoint)
  }

  it should "not pass single value point (unset value)" in {
    val pointProto = PopeyeTestUtils.createListPoint()
    val builder = Message.Point.newBuilder(pointProto)
    builder.setValueType(Message.Point.ValueType.FLOAT)
    intercept[IllegalArgumentException] {
      MessageUtil.validatePoint(builder.build())
    }
  }
}
