package popeye.transport.kafka

import akka.actor.{Props, ActorLogging, Actor}
import kafka.producer._
import java.util.Properties
import popeye.transport.proto.Message.Batch
import kafka.utils.Utils
import kafka.serializer.DefaultEncoder
import com.typesafe.config.Config
import popeye.transport.ConfigUtil._
import popeye.uuid.IdGenerator
import scala.collection.JavaConversions.{asScalaBuffer, asJavaIterable}
import popeye.transport.proto.Storage.Ensemble
import kafka.producer.KeyedMessage
import akka.actor.Status.Failure


object KafkaEventProducer {
  private def composeConfig(config: Config) = {
    config.getConfig("kafka.producer")
      .withFallback(config.getConfig("kafka"))
  }

  private def producerConfig(config: Config) = {
    val local: Config = config
    val props: Properties = new Properties()
    props.putAll(local)
    props.setProperty("zookeeper.connect", local.getString("zk.cluster"))
    props.setProperty("serializer.class", classOf[EnsembleEncoder].getName)
    props.setProperty("key.serializer.class", classOf[DefaultEncoder].getName)
    props.setProperty("compression.codec", "gzip")
    new ProducerConfig(props)
  }

  def props(config: Config)(implicit idGenerator: IdGenerator) = {
    val flatConfig: Config = composeConfig(config)
    Props(new KafkaEventProducer(flatConfig.getString("events.topic"), producerConfig(flatConfig), idGenerator))
  }
}

class KafkaEventProducer(topic: String,
                    config: ProducerConfig,
                    idGenerator: IdGenerator)
  extends Actor with ActorLogging {

  val producer = new Producer[Array[Byte], Ensemble](config)

  override def preStart() {
    log.debug("Starting producer")
    super.preStart()
  }

  override def postStop() {
    log.debug("Stopping producer")
    producer.close()
    super.postStop()
  }

  def receive = {

    case ProducePending(events, correlation) => {
      try {
        for {
          (part, list) <- events.getEventList
            .groupBy(ev => (ev.getMetric.hashCode() % 16) -> ev)

          batchId: Long = idGenerator.nextId()

          ensemble = Ensemble.newBuilder()
            .setBatchId(batchId)
            .addAllEvents(list)
            .build
        } yield {
          producer.send(
            KeyedMessage(topic, batchId.toString.getBytes, ensemble)
          )
          sender ! ProduceDone(correlation, batchId)
        }
      } catch {
        case e: Exception => sender ! Failure(e)
          sender ! ProduceFailed(correlation, e)
          throw e
      }
    }
  }
}

