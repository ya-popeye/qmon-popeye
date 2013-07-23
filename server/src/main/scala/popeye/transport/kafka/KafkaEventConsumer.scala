package popeye.transport.kafka

import popeye.Logging
import scala.util.{Failure, Success}
import kafka.serializer.{DefaultDecoder, Decoder}
import com.google.protobuf.InvalidProtocolBufferException
import akka.pattern.ask
import akka.actor._
import com.typesafe.config.Config
import kafka.consumer._
import java.util.Properties
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.Restart
import scala.Option
import scala.Some
import akka.actor.OneForOneStrategy
import popeye.transport.kafka.KafkaEventConsumer.ConsumerPair
import popeye.transport.ConfigUtil._
import popeye.transport.proto.Storage.Ensemble
import scala.collection.mutable
import akka.util.Timeout

/**
 * @author Andrey Stepachev
 */
object KafkaEventConsumer extends Logging {

  def consumerConfig(globalConfig: Config): ConsumerConfig = {
    val config: Config = globalConfig.getConfig("kafka.consumer")
    val consumerProps: Properties = config
    val timeout = globalConfig.getMilliseconds("kafka.consumer.timeout")
    consumerProps.put("consumer.timeout.ms", timeout.toString)
    consumerProps.put("group.id", globalConfig.getString("kafka.events.group"))
    new ConsumerConfig(consumerProps)
  }

  def props(config: Config, target: ActorRef) = {
    Props(
      new KafkaEventConsumer(
        config.getString("kafka.events.topic"),
        config.getString("kafka.events.group"),
        consumerConfig(config),
        target)
    )
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
        log.error("Did not get a valid stream from topic " + topic)
        None
      }
    }
    log.info(s"Consumer created")
    new ConsumerPair(consumerConnector, stream)
  }
}

class ConsumerInitializationException extends Exception
class BatchProcessingFailedException extends Exception

private case object Next

class KafkaEventConsumer(topic: String, group: String, config: ConsumerConfig, target: ActorRef) extends Actor with ActorLogging {

  import context._

  val pair: ConsumerPair = KafkaEventConsumer.createConsumer(topic, config)
  if (pair.consumer.isEmpty)
    throw new ConsumerInitializationException
  log.debug("Starting KafkaEventConsumer for group " + group + " and topic " + topic)
  val consumer = pair.consumer
  val connector = pair.connector
  lazy implicit val timeout: Timeout = new Timeout(system.settings.config.getMilliseconds("kafka.actor.timeout"), MILLISECONDS)
  lazy val maxPending = 10

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries=10, withinTimeRange = (5 minutes)) {
      case ex: BatchProcessingFailedException ⇒ Restart
    }


  override def preStart() {
    super.preStart()
    self ! Next
  }

  override def postStop() {
    log.debug("Stopping KafkaEventConsumer for group " + group + " and topic " + topic)
    connector.shutdown()
    super.postStop()
  }

  private def commit() = {
    // TODO: replace with more precise version, we should be able to commit particular (topic/part)
    connector.commitOffsets
  }

  def receive = {
    case ConsumeDone(id) =>
      if (log.isDebugEnabled)
        log.debug("Consumed {}", id)
      commit()
      self ! Next
    case ConsumeFailed(id, ex) =>
      log.error(ex, "Batch {} failed", id)
      throw new BatchProcessingFailedException
    case Next =>
      try {
        val iterator = consumer.get.iterator()
        if (iterator.hasNext()) {
          val msg = iterator.next()
          val batchId = msg.message.getBatchId
          val pos = ConsumeId(batchId, msg.offset, msg.partition)
          val me = self
          target ? ConsumePending(msg.message, pos) onComplete {
            case Success(x) =>
              me ! ConsumeDone(pos)
            case Failure(ex: Throwable) =>
              me ! ConsumeFailed(pos, ex)
          }
        }
      } catch {
        case ex: ConsumerTimeoutException => // ok
        case ex: Throwable =>
          log.error("Failed to consume", ex)
          throw ex
      }
  }
}




