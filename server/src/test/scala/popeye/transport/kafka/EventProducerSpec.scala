package popeye.transport.kafka

import popeye.transport.test.{AkkaTestKitSpec, KafkaServerTestSpec}
import scala.concurrent.duration._
import kafka.utils.Logging
import akka.testkit.TestActorRef
import com.typesafe.config.{ConfigFactory, Config}
import popeye.transport.proto.Message.{Batch, Tag, Event}
import com.google.protobuf.{ByteString => GByteString}
import akka.pattern.ask
import scala.compat.Platform
import akka.util.Timeout
import scala.concurrent.Await
import kafka.admin.CreateTopicCommand
import kafka.utils.TestUtils._
import kafka.message.MessageAndMetadata
import popeye.uuid.IdGenerator

/**
 * @author Andrey Stepachev
 */
class EventProducerSpec extends AkkaTestKitSpec("ProducerTest") with KafkaServerTestSpec with Logging {

  override def numServers() = 2

  val topic = "test"
  val group = "test"
  implicit val timeout: Timeout = 5 seconds
  implicit val generator: IdGenerator = new IdGenerator(1)

  "Producer" should "should publish events" in withKafkaServer() {
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)

    val config: Config = ConfigFactory.parseString(
      s"""
        |   kafka.consumer.auto.offset.reset=smallest
        |   kafka.consumer.group=$group
        |   kafka.producer.request.required.acks=1
        |   kafka.events.topic="$topic"
        |   kafka.zk.cluster="$zkConnect"
        |   kafka.consumer.timeout=35000
        |   kafka.producer.metadata.broker.list="$kafkaBrokersList"
      """.stripMargin).withFallback(ConfigFactory.load())
    val actor = TestActorRef(EventProducer.props(config))
    val future = ask(actor, PersistBatch(makeBatch())).mapTo[BatchPersisted]
    Await.result(future, timeout.duration)
    logger.debug(s"Got result ${future.value}, ready for consume")
    val consumer = EventConsumer.createConsumer(topic, EventConsumer.consumerConfig(config))
    try {
      val next = consumer.consumer.get.iterator().next()
      next must not be null
      logger.debug(s"Got message: ${next.message.toString}")
    } finally {
      logger.debug("Shutting down consumer")
      consumer.shutdown
    }
  }

  def makeBatch(): Batch = {
    Batch.newBuilder().addEvent(
      Event.newBuilder()
        .setIntValue(1)
        .setMetric("my.metric")
        .setTimestamp(Platform.currentTime)
        .addTags(Tag.newBuilder().setName("host").setValue("localhost").build())
        .build()
    ).build
  }
}
