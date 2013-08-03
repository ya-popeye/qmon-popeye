package popeye.transport.kafka

import org.scalatest.mock.MockitoSugar
import popeye.transport.test.{AkkaTestKitSpec, KafkaServerTestSpec}
import akka.testkit.TestActorRef
import org.hbase.async.{Bytes, KeyValue}
import java.util.Random
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => the}
import java.util
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._
import akka.util.Timeout
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import kafka.utils.TestUtils._
import kafka.admin.CreateTopicCommand
import popeye.{IdGenerator, ConfigUtil}
import popeye.transport.proto.PackedPoints
import popeye.test.PopeyeTestUtils._

/**
 * @author Andrey Stepachev
 */
class KafkaPointProducerTestSpec extends AkkaTestKitSpec("tsdb-writer") with KafkaServerTestSpec with MockitoSugar {

  val mockSettings = withSettings()
  //.verboseLogging()
  val idGenerator = new IdGenerator(1)
  implicit val rnd = new Random(1234)
  implicit val timeout: Timeout = 5 seconds
  implicit val generator: IdGenerator = new IdGenerator(1)
  implicit val metricRegistry = new MetricRegistry()
  val topic = "test"
  val group = "test"


  behavior of "TsdbPointConsumer"

  it should "consume" in withKafkaServer() {
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "")
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500, None)

    val config: Config = ConfigFactory.parseString(
      s"""
        |   zk.cluster = "$zkConnect"
        |   tsdb.consumer {
        |         auto.offset.reset=smallest
        |         group=$group
        |   }
        |   kafka.metadata.broker.list="$kafkaBrokersList"
        |   kafka.produce.message.min-bytes = 1
        |   kafka.produce.senders = 1
        |   kafka.points.topic="$topic"
      """.stripMargin).withFallback(ConfigUtil.loadSubsysConfig("pump")).resolve()
    val actor: TestActorRef[KafkaPointProducer] = TestActorRef(KafkaPointProducer.props(config, generator))
    val p = Promise[Long]()
    actor ! ProducePending(Some(p))(PackedPoints(makeBatch()))
    val done = Await.result(p.future, timeout.duration)
    actor.underlyingActor.metrics.batchCompleteMeter.count must be(1)
    system.shutdown()
  }

  def mkIdKeyValue(id: Long): util.ArrayList[KeyValue] = {
    val a = new util.ArrayList[KeyValue]()
    a.add(new KeyValue(util.Arrays.copyOf(Bytes.fromLong(id), 3),
      "id".getBytes, "name".getBytes, util.Arrays.copyOf(Bytes.fromLong(id), 3)))
    a
  }
}
