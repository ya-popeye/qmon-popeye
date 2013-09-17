package popeye.transport.kafka

import org.scalatest.mock.MockitoSugar
import popeye.transport.test.{AkkaTestKitSpec}
import akka.testkit.TestActorRef
import java.util.Random
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => the}
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._
import akka.util.Timeout
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import popeye.{IdGenerator, ConfigUtil}
import popeye.transport.proto.PackedPoints
import popeye.test.PopeyeTestUtils._
import kafka.producer.Producer

/**
 * @author Andrey Stepachev
 */
class KafkaPointProducerTestSpec extends AkkaTestKitSpec("tsdb-writer") with MockitoSugar {

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

  it should "consume" in {

    val config: Config = ConfigFactory.parseString(
      s"""
        |   kafka.broker.list="localhost:9092"
        |   kafka.producer.config += "popeye/transport/kafka/KafkaPointProducerTestSpec.properties"
        |   kafka.producer.senders = 1
        |   kafka.points.topic="$topic"
      """.stripMargin)
      .withFallback(ConfigFactory.parseResources("reference.conf"))
      .resolve()
    val producer = mock[Producer[Long, Array[Byte]]]
    val actor: TestActorRef[KafkaPointProducer] = TestActorRef(
      KafkaPointProducer.props(config, generator, new PopeyeKafkaProducerFactory {
        def newProducer(): Producer[Long, Array[Byte]] = producer
      }))
    val p = Promise[Long]()
    actor ! ProducePending(Some(p))(PackedPoints(makeBatch()))
    val done = Await.result(p.future, timeout.duration)
    actor.underlyingActor.metrics.batchCompleteMeter.count must be(1)
    system.shutdown()
  }
}
