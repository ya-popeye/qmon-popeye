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
import popeye.uuid.IdGenerator
import com.codahale.metrics.MetricRegistry
import popeye.BufferedFSM.Flush

/**
 * @author Andrey Stepachev
 */
class EventProducerSpec extends AkkaTestKitSpec("ProducerTest") with KafkaServerTestSpec with Logging {

  override def numServers() = 2

  val topic = "test"
  val group = "test"
  implicit val timeout: Timeout = 5 seconds
  implicit val generator: IdGenerator = new IdGenerator(1)
  implicit val metricRegistry = new MetricRegistry

  "Producer" should "should publish events" in withKafkaServer() {
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "")
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500, None)

    val config: Config = ConfigFactory.parseString(
      s"""
        |   kafka.consumer.auto.offset.reset=smallest
        |   kafka.consumer.group=$group
        |   kafka.events.topic="$topic"
        |   kafka.consumer.zookeeper.connect="$zkConnect"
        |   kafka.producer.metadata.broker.list="$kafkaBrokersList"
      """.stripMargin).withFallback(ConfigFactory.load())
    val actor = TestActorRef(KafkaEventProducer.props(config, generator))
    val future = ask(actor, ProducePending(makeBatch(), 123)).mapTo[ProduceDone]
    actor ! Flush()
    Await.result(future, timeout.duration)
    logger.debug(s"Got result ${future.value}, ready for consume")
    val consumer = KafkaEventConsumer.createConsumer(topic, KafkaEventConsumer.consumerConfig(config))
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
