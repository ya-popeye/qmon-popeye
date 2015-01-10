package popeye.pipeline.server.telnet

import org.scalatest.{Matchers, FlatSpec}
import popeye.proto.Message

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import popeye.test.PopeyeTestUtils.time

import scala.collection.JavaConverters._

/**
 * @author Andrey Stepachev
 */
class TelnetCommandsSpec extends FlatSpec with Matchers {

  "TelnetCommands.parseLong" should "handle longs" in {
    val a = Array("978426007", "8")
    a.forall { l =>
      val parsed = TelnetCommands.parseLong(l)
      val actual = l.toLong
      parsed == actual
    } should be(true)
  }

  behavior of "TelnetCommands.parsePoint"

  it should "parse single-element float list value" in {
    val point = TelnetCommands.parsePoint("put proc.loadavg.5m 1288947942 [1.62] host=bar cluster=popeye".split(" "))
    point.getFloatListValueCount should equal(1)
    point.getFloatListValue(0) should equal(1.62f)
    point.getValueType should equal(Message.Point.ValueType.FLOAT_LIST)
  }

  it should "parse single-element long list value" in {
    val point = TelnetCommands.parsePoint("put proc.loadavg.5m 1288947942 [1] host=bar cluster=popeye".split(" "))
    point.getIntListValueCount should equal(1)
    point.getIntListValue(0) should equal(1l)
    point.getValueType should equal(Message.Point.ValueType.INT_LIST)
  }

  it should "parse list value" in {
    val point = TelnetCommands.parsePoint("put proc.loadavg.5m 1288947942 [1,2,3,4,5] host=bar cluster=popeye".split(" "))
    point.getIntListValueList should equal(Seq(1, 2, 3, 4, 5).map(_.toLong).asJava)
    point.getValueType should equal(Message.Point.ValueType.INT_LIST)
  }

  it should "parse mixed list value" in {
    val point = TelnetCommands.parsePoint("put proc.loadavg.5m 1288947942 [1,2,3.0,4,5] host=bar cluster=popeye".split(" "))
    point.getIntListValueCount should equal(0)
    point.getFloatListValueList should equal(Seq(1, 2, 3, 4, 5).map(_.toFloat).asJava)
    point.getValueType should equal(Message.Point.ValueType.FLOAT_LIST)
  }

  it should "throw exception on non ']' end symbol in list value" in {
    intercept[IllegalArgumentException] {
      TelnetCommands.parsePoint("put proc.loadavg.5m 1288947942 [0[ host=bar cluster=popeye".split(" "))
    }
  }

  ignore should "have reasonable performance" in {
    val metrics = Vector(
      "proc.loadavg.5m",
      "yt.rpc.server.request_time.local_wait.avg",
      "proc.net.bytes"
    )
    val random = new Random
    val putStrings: mutable.ArrayBuffer[mutable.ArrayBuffer[String]] = (0 to 10000).map {
      _ =>
        val metric = metrics(random.nextInt(metrics.size))
        val timestamp = random.nextInt(1407922793).toString
        val value = (0 to 1000).map(_ => random.nextInt(100000000).toString).mkString("[", ",", "]")
        def copyString(str: String) = new String(str.toCharArray)
        ArrayBuffer(copyString("put"), metric, timestamp, value, copyString("host=bar"), copyString("cluster=popeye"))
    }(scala.collection.breakOut)
    for (_ <- 1 to 1000) {
      val parseTime = time {
        putStrings.foreach(TelnetCommands.parsePoint)
      }
      println(parseTime)
    }
  }
}
