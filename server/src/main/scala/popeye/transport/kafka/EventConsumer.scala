package popeye.transport.kafka

import scala.util.{Failure, Success}
import kafka.utils.{Logging, VerifiableProperties}
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
import popeye.transport.kafka.EventConsumer.ConsumerPair
import popeye.transport.ConfigUtil._
import popeye.transport.proto.Storage.Ensemble
import scala.collection.mutable
import akka.util.Timeout

/**
 * @author Andrey Stepachev
 */
object EventConsumer extends Logging {

  def consumerConfig(config: Config): ConsumerConfig = {
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

  def props(config: Config, target: ActorRef) = {
    val flatConfig = composeConfig(config)
    Props(new EventConsumer(
      flatConfig.getString("events.topic"),
      flatConfig.getString("events.group"),
      consumerConfig(config),
      target))
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

private case object Go

case class ConsumeId(batchId: Long, offset: Long, partition: Long)

case class ConsumePending(data: Ensemble, id: ConsumeId)

case class ConsumeDone(id: ConsumeId)

case class ConsumeFailed(id: ConsumeId, cause: Throwable)

class EventConsumer(topic: String, group: String, config: ConsumerConfig, target: ActorRef) extends Actor with ActorLogging {

  import context._

  val pair: ConsumerPair = EventConsumer.createConsumer(topic, config)
  if (pair.consumer.isEmpty)
    throw new ConsumerInitializationException
  log.debug("Starting EventConsumer for group " + group + " and topic " + topic)
  val consumer = pair.consumer
  val connector = pair.connector
  val pending = new mutable.TreeSet[Long]()
  val complete = new mutable.TreeSet[Long]()
  implicit val timeout: Timeout = 30 seconds

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = (1 minutes)) {
      case _ ⇒ Restart
    }


  override def postStop() {
    log.debug("Stopping EventConsumer for group " + group + " and topic " + topic)
    connector.shutdown()
    super.postStop()
  }

  private def commit() = {
    // TODO: replace with more precise version, we should be able to commit particular (topic/part)
    connector.commitOffsets
  }

  def receive = {
    case ConsumeDone(id) =>
      if (pending.firstKey == id.offset) {
        pending.remove(id.offset)
        complete.clear()
        commit()
      } else {
        pending.remove(id.offset)
        complete.add(id.offset)
      }
    case ConsumeFailed(id, ex) =>
      // TODO: place failed batch somewhere, right now we simply drop it
      log.error(ex, "Batch {} failed", id)
      pending.remove(id.offset)
      if (complete.isEmpty)
        commit()
    case Go =>
      if (pending.size >= config.queuedMaxMessages) {
        log.warning("Queue is full: " + pending.size)
        context.system.scheduler.scheduleOnce(5 seconds, self, Go)
      } else {
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
        }
        self ! Go
      }
  }

}




