package popeye.transport.kafka

import kafka.utils.{Logging, VerifiableProperties}
import kafka.serializer.{DefaultDecoder, Decoder}
import popeye.transport.proto.Message.Event
import com.google.protobuf.InvalidProtocolBufferException
import akka.actor._
import com.typesafe.config.Config
import kafka.consumer._
import java.util.Properties
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.Restart
import scala.Option
import scala.Some
import akka.actor.OneForOneStrategy
import popeye.transport.kafka.EventConsumer.ConsumerPair
import popeye.transport.ConfigUtil._

/**
 * @author Andrey Stepachev
 */
object EventConsumer extends Logging {
  def props() = {
    Props[EventConsumer]()
  }

  def unwrapConfig(config: Config): Config = {

    config.getConfig("kafka.consumer")
      .withFallback(config.getConfig("kafka"))
  }

  def fromConfig(config: Config): Props = {
    Props(new EventProducer(unwrapConfig(config)))
  }

  class ConsumerPair(val connector: ConsumerConnector, val consumer: Option[KafkaStream[Array[Byte], Event]]) {
    def shutdown = {
      connector.shutdown()
    }
  }

  def createConsumer(topic: String, group: String, kafkaConfig: Config): ConsumerPair = {
    val consumerProps: Properties = kafkaConfig
    consumerProps.put("zookeeper.connect", kafkaConfig.getString("zk.cluster"))
    consumerProps.put("consumer.timeout.ms", kafkaConfig.getString("consumer.timeout"))
    consumerProps.put("group.id", group)
    val consumerConfig = new ConsumerConfig(consumerProps)
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)

    val topicThreadCount = Map((topic, 1))
    val topicMessageStreams = consumerConnector.createMessageStreams(
      topicThreadCount,
      new DefaultDecoder(),
      new EventDecoder()
    )
    val streams = topicMessageStreams.get(topic)

    val stream = streams match {
      case Some(List(stream)) => Some(stream)
      case _ => {
        logger.error("Did not get a valid stream from topic " + topic)
        None
      }
    }
    logger.info(s"Consumer created")
    new ConsumerPair(consumerConnector, stream)
  }
}

class ConsumerInitializationException extends Exception

class EventConsumer(kafkaConfig: Config) extends Actor with ActorLogging {

  val topic = kafkaConfig.getString("events.topic")
  val group = kafkaConfig.getString("consumer.group")
  var connector: Option[ConsumerConnector] = None
  var consumer: Option[KafkaStream[Array[Byte], Event]] = None

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = (1 minutes)) {
      case _ ⇒ Restart
    }

  override def preStart() {
    val pair: ConsumerPair = EventConsumer.createConsumer(topic, group, kafkaConfig)
    if (pair.consumer.isEmpty)
      throw new ConsumerInitializationException
    log.debug("Starting EventConsumer for group " + group + " and topic " + topic)
    consumer = pair.consumer
    connector = Some(pair.connector)
  }

  override def postStop() {
    super.postStop()
    log.debug("Stopping EventConsumer for group " + group + " and topic " + topic)
    connector foreach {
      c => c.shutdown()
    }
    connector = None
    consumer = None
  }

  def receive = {
    case _ =>
      try {
        val iterator: ConsumerIterator[Array[Byte], Event] = consumer.get.iterator()
        if (iterator.hasNext()) {
          val msg = iterator.next()
          log.debug(msg.toString)
        }
      } catch {
        case ex: ConsumerTimeoutException => // ok
      }
  }

}

class EventDecoder(props: VerifiableProperties = null) extends Decoder[Event] with Logging {

  def fromBytes(bytes: Array[Byte]): Event = {
    try {
      Event.parseFrom(bytes)
    } catch {
      case ex: InvalidProtocolBufferException =>
        logger.error("Can't parse message", ex)
        return null
    }
  }
}


