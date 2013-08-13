package popeye.transport.legacy

import org.scalatest.mock.MockitoSugar
import popeye.transport.test.{AkkaTestKitSpec, KafkaServerTestSpec}
import akka.testkit.{TestProbe, TestActorRef}
import org.hbase.async.{Bytes, KeyValue}
import java.util.Random
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => the}
import java.util
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._
import akka.util.{ByteString, Timeout}
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import kafka.utils.TestUtils._
import kafka.admin.CreateTopicCommand
import popeye.{IdGenerator, ConfigUtil}
import popeye.transport.proto.PackedPoints
import popeye.test.PopeyeTestUtils._
import akka.actor.{Deploy, Props}
import akka.io.TcpPipelineHandler.{Init, WithinActorContext}
import akka.io.{BackpressureBuffer, TcpReadWriteAdapter, TcpPipelineHandler}
import popeye.transport.kafka.{ProduceDone, ProducePending}
import akka.event.NoLogging
import akka.io.Tcp.ConfirmedClose
import java.io.ByteArrayOutputStream
import java.util.zip.{Deflater, DeflaterOutputStream}
import popeye.transport.CompressionDecoder.{Gzip, Snappy, Codec}

/**
 * @author Andrey Stepachev
 */
class TsdbTelnetHandlerTestSpec extends AkkaTestKitSpec("tsdb-server") with MockitoSugar {

  val mockSettings = withSettings()
  //.verboseLogging()
  implicit val rnd = new Random(1234)
  implicit val timeout: Timeout = 5 seconds
  implicit val metricRegistry = new MetricRegistry()
  implicit val tsdbMetrics = new TsdbTelnetMetrics(metricRegistry)

  private val init = TcpPipelineHandler.withLogger(NoLogging, new TcpReadWriteAdapter)

  private def initActors(batchSize: Int = 1) =  {
    val config: Config = ConfigFactory.parseString(
      s"""
        | legacy.tsdb.high-watermark = 2000
        | legacy.tsdb.low-watermark = 1000
        | legacy.tsdb.batchSize = $batchSize
      """.stripMargin)
      .withFallback(ConfigUtil.loadSubsysConfig("pump"))
      .resolve()

    val kafka = TestProbe()
    val connection = TestProbe()
    val actor: TestActorRef[TsdbTelnetHandler] = TestActorRef(Props(
      new TsdbTelnetHandler(init, connection.ref, kafka.ref, config, tsdbMetrics))
      .withDeploy(Deploy.local))
    (connection, kafka, actor)
  }

  val plainCmd = ByteString.fromString("put metric.name 1375173810 0 host=localhost\r\n")
  val encodedCmd = encode(plainCmd, Gzip())
  val snappedCmd = encode(plainCmd, Snappy())
  val deflateCmd = ByteString.fromString(s"deflate ${encodedCmd.length}\r\n")
  val snappyCmd = ByteString.fromString(s"snappy ${snappedCmd.length}\r\n")
  val commitCmd = ByteString.fromString("commit 1\r\n")

  behavior of "TsdbTelnetHandler"

  it should "plain interaction" in {
    val (connection, kafka, actor) = initActors(2)
    actor ! init.Event(plainCmd.take(10))
    actor ! init.Event(plainCmd.drop(10))
    actor ! init.Event(commitCmd)
    validate(connection, kafka, actor)
  }

  it should "process very fragmented buffer" in {
    val (connection, kafka, actor) = initActors(2)

    for (b <- plainCmd ++ commitCmd) {
      actor ! init.Event(ByteString.fromArray(Array(b)))
    }
    validate(connection, kafka, actor)
  }

  it should "process one big plain buffer" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! init.Event((plainCmd ++ commitCmd).compact)
    validate(connection, kafka, actor)
  }

  it should "deflated interaction" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! init.Event(deflateCmd)
    actor ! init.Event(encodedCmd.take(10))
    actor ! init.Event(encodedCmd.drop(10))
    actor ! init.Event(commitCmd)
    validate(connection, kafka, actor)
  }

  it should "snappy interaction" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! init.Event(snappyCmd)
    actor ! init.Event(snappedCmd.take(10))
    actor ! init.Event(snappedCmd.drop(10))
    actor ! init.Event(commitCmd)
    validate(connection, kafka, actor)
  }

  it should "process very fragmented deflated  buffer" in {
    val (connection, kafka, actor) = initActors(2)

    for (b <- deflateCmd ++ encodedCmd ++ commitCmd) {
      actor ! init.Event(ByteString.fromArray(Array(b)))
    }
    validate(connection, kafka, actor)
  }

  it should "process very fragmented snapped  buffer" in {
    val (connection, kafka, actor) = initActors(2)

    for (b <- snappyCmd ++ snappedCmd ++ commitCmd) {
      actor ! init.Event(ByteString.fromArray(Array(b)))
    }
    validate(connection, kafka, actor)
  }

  it should "process one big deflated buffer" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! init.Event((deflateCmd ++ encodedCmd ++ commitCmd).compact)
    validate(connection, kafka, actor)
  }

  it should "process one big snapped buffer" in {
    val (connection, kafka, actor) = initActors(2)

    actor ! init.Event((snappyCmd ++ snappedCmd ++ commitCmd).compact)
    validate(connection, kafka, actor)
  }

  def validate(connection: TestProbe, kafka: TestProbe, actor: TestActorRef[TsdbTelnetHandler]) {
    kafka.expectMsgPF(){
      case p @ ProducePending(promise) =>
        assert(p.data.size == 1)
        promise.get.success(123)
    }
    receiveWhile(){
      case p @ ProduceDone(corrs, batchId) =>
        assert(corrs.contains(1))
        assert(batchId == 123)
    }

    actor ! init.Event(ByteString.fromString("exit\r\n"))
    connection.expectMsgPF(){
      case ConfirmedClose =>
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
