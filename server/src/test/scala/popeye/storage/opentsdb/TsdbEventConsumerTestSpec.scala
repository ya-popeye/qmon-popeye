package popeye.storage.opentsdb

import org.scalatest.mock.MockitoSugar
import popeye.transport.test.{AkkaTestKitSpec, KafkaServerTestSpec}
import akka.testkit.TestActorRef
import org.hbase.async.{Bytes, KeyValue, HBaseClient}
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
import akka.pattern.ask
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import kafka.admin.CreateTopicCommand
import kafka.utils.TestUtils._
import scala.Some
import popeye.transport.kafka.{ProduceDone, ProducePending, KafkaEventProducer}

/**
 * @author Andrey Stepachev
 */
class TsdbEventConsumerTestSpec extends AkkaTestKitSpec("tsdb-writer") with KafkaServerTestSpec with MockitoSugar {

  val mockSettings = withSettings()
  //.verboseLogging()
  val idGenerator = new IdGenerator(1)
  val ts = new AtomicInteger(1234123412)
  implicit val timeout: Timeout = 5 seconds
  implicit val generator: IdGenerator = new IdGenerator(1)
  implicit val metricRegistry = new MetricRegistry()
  val topic = "test"
  val group = "test"


  behavior of "TsdbEventConsumer"

  it should "consume" in withKafkaServer() {
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "")
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500, None)

    val hbc = mock[HBaseClient](mockSettings)
    when(hbc.get(any())).thenReturn(Deferred.fromResult(mkIdKeyValue(1)))
    when(hbc.put(any())).thenReturn(Deferred.fromResult(new Object))
    val config: Config = ConfigFactory.parseString(
      s"""
        |   tsdb.consumer {
        |         auto.offset.reset=smallest
        |         group=$group
        |         zookeeper.connect="$zkConnect"
        |   }
        |   kafka.producer.metadata.broker.list="$kafkaBrokersList"
        |   kafka.points.topic="$topic"
      """.stripMargin).withFallback(ConfigFactory.load())
    val actor = TestActorRef(KafkaEventProducer.props(config, generator))
    val future = ask(actor, ProducePending(123)(makeBatch())).mapTo[ProduceDone]
    Await.result(future, timeout.duration)
    logger.debug(s"Got result ${future.value}, ready for consume")

    val consumer: TestActorRef[TsdbEventConsumer] = TestActorRef(TsdbEventConsumer.props(config, Some(hbc)))
    consumer.underlyingActor.metrics.batchCompleteHist.count must be(1)
    system.shutdown()
  }

  val rnd = new Random(12345)

  def mkEvents(msgs: Int = 2): Traversable[Event] = {
    for {
      i <- 0 to msgs - 1
    } yield {
      mkEvent()
    }
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

  def makeBatch(): Seq[Event] = {
    mkEvents().toSeq
  }


  def mkIdKeyValue(id: Long): util.ArrayList[KeyValue] = {
    val a = new util.ArrayList[KeyValue]()
    a.add(new KeyValue(util.Arrays.copyOf(Bytes.fromLong(id), 3),
      "id".getBytes, "name".getBytes, util.Arrays.copyOf(Bytes.fromLong(id), 3)))
    a
  }
}
