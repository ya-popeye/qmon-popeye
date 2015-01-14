package popeye.pipeline.kafka

import akka.testkit.TestActorRef
import akka.util.Timeout
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import java.util.Random
import kafka.producer.Producer
import org.mockito.Matchers.{eq => the}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import popeye.IdGenerator
import popeye.test.AkkaTestKitSpec
import popeye.test.PopeyeTestUtils._
import popeye.proto.PackedPoints
import scala.concurrent.duration._
import scala.concurrent.{Promise, Await}
import popeye.pipeline.{PointsSinkFactory, PointsSink}

/**
 * @author Andrey Stepachev
 */
class KafkaPointProducerSpec extends AkkaTestKitSpec("tsdb-writer") with MockitoSugar {

  val mockSettings = withSettings()
  //.verboseLogging()
  val idGenerator = new IdGenerator(1)
  implicit val rnd = new Random(1234)
  implicit val timeout: Timeout = 5 seconds
  implicit val metricRegistry = new MetricRegistry()
  val topic = "test"
  val group = "test"


  behavior of "TsdbPointConsumer"

  it should "consume" in {

    val config: Config = ConfigFactory.parseString(
      s"""
        |   broker.list="localhost:9092"
        |   producer.config += "popeye/pipeline/kafka/KafkaPointProducerSpec.properties"
        |   producer.workers = 1
        |   producer.low-watermark = 1
        |   tick=200ms
        |   topic="$topic"
      """.stripMargin)
      .withFallback(ConfigFactory.parseResources("reference.conf"))
      .resolve()
    val producer = mock[PointsSink]
    val producerConfig: Config =
      config.withFallback(config.getConfig("common.popeye.pipeline.kafka"))
    val actor: TestActorRef[KafkaPointsProducer] = TestActorRef(
      KafkaPointsProducer.props("kafka",
        new KafkaPointsProducerConfig(producerConfig),
        idGenerator, new PointsSinkFactory {
        def newSender(topic: String): PointsSink = producer
      }, metricRegistry))
    val p = Promise[Long]()
    KafkaPointsProducer.producePoints(actor, Some(p), makeBatch() :_*)
    val done = Await.result(p.future, timeout.duration)
    actor.underlyingActor.metrics.batchCompleteMeter.count should be(1)
    system.shutdown()
  }
}
