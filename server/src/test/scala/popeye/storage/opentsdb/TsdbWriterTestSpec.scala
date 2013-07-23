package popeye.storage.opentsdb

import org.scalatest.mock.MockitoSugar
import popeye.transport.test.{AkkaTestKitSpec, KafkaServerTestSpec}
import akka.testkit.TestActorRef
import akka.pattern.ask
import org.hbase.async.{Bytes, KeyValue, HBaseClient}
import net.opentsdb.core.TSDB
import popeye.transport.kafka.{ConsumeDone, ConsumeId, ConsumePending}
import popeye.transport.proto.Storage.Ensemble
import popeye.uuid.IdGenerator
import popeye.transport.proto.Message.{Tag, Event}
import java.util.Random
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => the, any}
import com.stumbleupon.async.Deferred
import java.util
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.util.Timeout
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.Props

/**
 * @author Andrey Stepachev
 */
class TsdbWriterTestSpec extends AkkaTestKitSpec("tsdb-writer") with KafkaServerTestSpec with MockitoSugar {

  val mockSettings = withSettings()
  //.verboseLogging()
  val idGenerator = new IdGenerator(1)
  val ts = new AtomicInteger(1234123412)
  implicit val timeout: Timeout = 5 seconds
  implicit val metricRegistry = new MetricRegistry()

  behavior of "TsdbWriter"


  it should "handle events" in {
    val hbc = mock[HBaseClient](mockSettings)
    when(hbc.get(any())).thenReturn(Deferred.fromResult(mkIdKeyValue(1)))
    when(hbc.put(any())).thenReturn(Deferred.fromResult(new Object))
    val tsdb = new TSDB(hbc, "tsdb", "tsdb-uid")
    val config: Config = ConfigFactory.parseString(
      s"""
        |
      """.stripMargin).withFallback(ConfigFactory.load())

    val writer = TestActorRef(Props(new TsdbWriter(config, tsdb)), "tsdb-writer")
    val ensemble: Ensemble = mkEnesemble()
    val id: ConsumeId = ConsumeId(ensemble.getBatchId, 1000, 1)
    val future = writer ? ConsumePending(ensemble, id)
    //writer ! Flush()
    val result = Await.result(future, timeout.duration).asInstanceOf[ConsumeDone]
    result.id must be(id)
  }

  val rnd = new Random(12345)

  def mkEnesemble(msgs: Int = 2): Ensemble = {
    val b = Ensemble.newBuilder()
      .setBatchId(idGenerator.nextId())
      .setPartition(1)
    for (i <- 0 to msgs - 1) {
      b.addEvents(i, mkEvent())
    }
    b.build()
  }

  def mkEvent(): Event = {
    Event.newBuilder()
      .setTimestamp(ts.getAndIncrement)
      .setIntValue(rnd.nextLong())
      .setMetric("proc.net.bytes")
      .addTags(Tag.newBuilder()
      .setName("host")
      .setValue("localhost")
    ).build()
  }

  def mkIdKeyValue(id: Long): util.ArrayList[KeyValue] = {
    val a = new util.ArrayList[KeyValue]()
    a.add(new KeyValue(util.Arrays.copyOf(Bytes.fromLong(id), 3),
      "id".getBytes, "name".getBytes, util.Arrays.copyOf(Bytes.fromLong(id), 3)))
    a
  }
}
