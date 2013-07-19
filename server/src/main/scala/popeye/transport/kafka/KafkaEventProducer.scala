package popeye.transport.kafka

import akka.actor._
import kafka.producer._
import java.util.Properties
import kafka.serializer.DefaultEncoder
import com.typesafe.config.Config
import popeye.transport.ConfigUtil._
import popeye.uuid.IdGenerator
import scala.collection.JavaConversions.{asScalaBuffer, asJavaIterable}
import popeye.transport.proto.Storage.Ensemble
import akka.routing.FromConfig
import kafka.producer.KeyedMessage
import akka.actor.Status.Failure
import com.codahale.metrics.MetricRegistry


object KafkaEventProducer {

  def start(config: Config, idGenerator: IdGenerator)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    system.actorOf(KafkaEventProducer.props(config, idGenerator)
      .withRouter(FromConfig()), "kafka-producer")
  }

  def producerConfig(globalConfig: Config): ProducerConfig = {
    val config: Config = globalConfig.getConfig("kafka.producer")
    val producerProps: Properties = config
    producerProps.setProperty("zookeeper.connect", globalConfig.getString("kafka.zk.cluster"))
    producerProps.setProperty("serializer.class", classOf[EnsembleEncoder].getName)
    producerProps.setProperty("key.serializer.class", classOf[DefaultEncoder].getName)
    new ProducerConfig(producerProps)
  }

  def props(config: Config, idGenerator: IdGenerator) = {
    Props(new KafkaEventProducer(
      config.getString("kafka.events.topic"),
      producerConfig(config),
      idGenerator,
      config.getInt("kafka.events.partition")))
  }
}

class KafkaEventProducer(topic: String,
                         config: ProducerConfig,
                         idGenerator: IdGenerator,
                         partitions: Int)
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
            .groupBy(ev => (ev.getMetric.hashCode() % partitions) -> ev)

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

