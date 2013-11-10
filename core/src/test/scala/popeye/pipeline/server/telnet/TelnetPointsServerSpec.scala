package popeye.transport.server.telnet

import popeye.transport.compression.CompressionDecoder
import CompressionDecoder.{Gzip, Snappy, Codec}
import akka.actor.{Deploy, Props}
import akka.io.Tcp
import akka.testkit.{TestProbe, TestActorRef}
import akka.util.{ByteString, Timeout}
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import java.io.ByteArrayOutputStream
import java.util.Random
import org.mockito.Matchers.{eq => the}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import popeye.ConfigUtil
import popeye.pipeline.DispatcherProtocol.Pending
import popeye.test.PopeyeTestUtils
import popeye.transport.test.AkkaTestKitSpec
import scala.concurrent.duration._

/**
 * @author Andrey Stepachev
 */
class TelnetPointsServerSpec extends AkkaTestKitSpec("tsdb-server") with MockitoSugar {

  val mockSettings = withSettings()
  //.verboseLogging()
  implicit val rnd = new Random(1234)
  implicit val timeout: Timeout = 5 seconds
  implicit val metricRegistry = new MetricRegistry()
  implicit val tsdbMetrics = new TsdbTelnetMetrics(metricRegistry)

  private def initActors(batchSize: Int = 1) = {
    val config: Config = ConfigFactory.parseString(
      s"""
        | server.telnet.high-watermark = 1
        | server.telnet.low-watermark = 0
        | server.telnet.produce-timeout = 10s
        | server.telnet.batchSize = $batchSize
      """.stripMargin)
      .withFallback(ConfigUtil.loadSubsysConfig("slicer"))
      .resolve()

    val kafka = TestProbe()
    val connection = TestProbe()
    val actor: TestActorRef[TsdbTelnetHandler] = TestActorRef(
      Props.apply(new TsdbTelnetHandler(connection.ref, kafka.ref, config, tsdbMetrics))
        .withDeploy(Deploy.local))
    (connection, kafka, actor)
  }


  val plainCmd = ByteString.fromString(PopeyeTestUtils.telnetCommand(
    PopeyeTestUtils.mkEvent(List("metric.name"), List("localhost"))
  ) + "\r\n")

  val encodedCmd = encode(plainCmd, Gzip())
  val snappedCmd = encode(plainCmd, Snappy())
  val deflateCmd = ByteString.fromString(s"deflate ${encodedCmd.length}\r\n")
  val snappyCmd = ByteString.fromString(s"snappy ${snappedCmd.length}\r\n")
  val commitCmd = ByteString.fromString("commit 1\r\n")

  behavior of "TsdbTelnetHandler"

  it should "plain interaction" in {
    val (connection, kafka, actor) = initActors(2)
    actor ! Tcp.Received(plainCmd.take(10))
    actor ! Tcp.Received(plainCmd.drop(10))
    expectNoMsg()
    actor ! Tcp.Received(commitCmd)
    validate(connection, kafka, actor)
  }

  it should "process very fragmented buffer" in {
    val (connection, kafka, actor) = initActors(2)

    for (b <- plainCmd ++ commitCmd) {
      actor ! Tcp.Received(ByteString.fromArray(Array(b)))
    }
    validate(connection, kafka, actor)
  }

  it should "process one big plain buffer" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! Tcp.Received((plainCmd ++ commitCmd).compact)
    validate(connection, kafka, actor)
  }

  it should "deflated interaction" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! Tcp.Received(deflateCmd)
    actor ! Tcp.Received(encodedCmd.take(10))
    actor ! Tcp.Received(encodedCmd.drop(10))
    expectNoMsg()
    actor ! Tcp.Received(commitCmd)
    validate(connection, kafka, actor)
  }

  it should "snappy interaction" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! Tcp.Received(snappyCmd)
    actor ! Tcp.Received(snappedCmd.take(10))
    actor ! Tcp.Received(snappedCmd.drop(10))
    expectNoMsg()
    actor ! Tcp.Received(commitCmd)
    validate(connection, kafka, actor)
  }

  it should "process very fragmented deflated  buffer" in {
    val (connection, kafka, actor) = initActors(2)

    for (b <- deflateCmd ++ encodedCmd ++ commitCmd) {
      actor ! Tcp.Received(ByteString.fromArray(Array(b)))
    }
    validate(connection, kafka, actor)
  }

  it should "process very fragmented snapped  buffer" in {
    val (connection, kafka, actor) = initActors(2)

    for (b <- snappyCmd ++ snappedCmd ++ commitCmd) {
      actor ! Tcp.Received(ByteString.fromArray(Array(b)))
    }
    validate(connection, kafka, actor)
  }

  it should "process one big deflated buffer" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! Tcp.Received((deflateCmd ++ encodedCmd ++ commitCmd).compact)
    validate(connection, kafka, actor)
  }

  it should "process one big snapped buffer" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! Tcp.Received((snappyCmd ++ snappedCmd ++ commitCmd).compact)
    validate(connection, kafka, actor)
  }

  def validate(connection: TestProbe, kafka: TestProbe, actor: TestActorRef[TsdbTelnetHandler]) {
    kafka.expectMsgPF() {
      case p@Pending(promise) =>
//        assert(p.event.size == 1)  // TODO: fix this
        promise.get.success(123)
    }
    fishForMessage() {
      case Tcp.Write(bstr, _) =>
        bstr.utf8String.contains("1 = 123")
    }

    actor ! Tcp.Received(ByteString.fromString("exit\r\n"))
    connection.expectMsgPF() {
      case Tcp.Close =>
    }
  }

  def encode(s: String, codec: Codec): ByteString = {
    encode(ByteString.fromString(s), codec)
  }

  def encode(s: ByteString, codec: Codec): ByteString = {
    val data: ByteArrayOutputStream = new ByteArrayOutputStream()
    val deflater = codec.makeOutputStream(data)
    deflater.write(s.toArray)
    deflater.close()
    ByteString.fromArray(data.toByteArray)
  }

}
