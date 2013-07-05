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
import popeye.transport.proto.Storage.Ensemble
import popeye.uuid.IdGenerator

/**
 * @author Andrey Stepachev
 */
object EventConsumer extends Logging {

  def consumerConfig(config:Config): ConsumerConfig = {
    val flatConfig: Config = composeConfig(config)
    val consumerProps: Properties = flatConfig
    consumerProps.put("zookeeper.connect", flatConfig.getString("zk.cluster"))
    consumerProps.put("consumer.timeout.ms", flatConfig.getString("timeout"))
    consumerProps.put("group.id", flatConfig.getString("events.group"))
    new ConsumerConfig(consumerProps)
  }

  private def composeConfig(config: Config): Config = {
    config.getConfig("kafka.consumer")
      .withFallback(config.getConfig("kafka"))
  }

  def props(config:Config) = {
    val flatConfig = composeConfig(config)
    Props(new EventConsumer(
      flatConfig.getString("events.topic"),
      flatConfig.getString("events.group"),
      consumerConfig(config)))
  }

  class ConsumerPair(val connector: ConsumerConnector, val consumer: Option[KafkaStream[Array[Byte], Ensemble]]) {
    def shutdown = {
      connector.shutdown()
    }
  }

  def createConsumer(topic: String, config: ConsumerConfig): ConsumerPair = {
    val consumerConnector: ConsumerConnector = Consumer.create(config)

    val topicThreadCount = Map((topic, 1))
    val topicMessageStreams = consumerConnector.createMessageStreams(
      topicThreadCount,
      new DefaultDecoder(),
      new EnsembleDecoder()
    )
    val streams = topicMessageStreams.get(topic)

    val stream = streams match {
      case Some(List(s)) => Some(s)
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

class EventConsumer(topic: String, group: String, config: ConsumerConfig) extends Actor with ActorLogging {

  val pair: ConsumerPair = EventConsumer.createConsumer(topic, config)
  if (pair.consumer.isEmpty)
    throw new ConsumerInitializationException
  log.debug("Starting EventConsumer for group " + group + " and topic " + topic)
  val consumer = pair.consumer
  val connector = pair.connector
  var finished = false

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = (1 minutes)) {
      case _ ⇒ Restart
    }


  override def postStop() {
    log.debug("Stopping EventConsumer for group " + group + " and topic " + topic)
    connector.shutdown()
    super.postStop()
  }

  def receive = {
    case _ =>
      try {
        val iterator = consumer.get.iterator()
        if (iterator.hasNext()) {
          val msg = iterator.next()
          log.debug(msg.toString)
        }
      } catch {
        case ex: ConsumerTimeoutException => // ok
      }
  }

}

class EnsembleDecoder(props: VerifiableProperties = null) extends Decoder[Ensemble] with Logging {

  def fromBytes(bytes: Array[Byte]): Ensemble = {
    try {
      Ensemble.parseFrom(bytes)
    } catch {
      case ex: InvalidProtocolBufferException =>
        logger.error("Can't parse message", ex)
        return null
    }
  }
}


