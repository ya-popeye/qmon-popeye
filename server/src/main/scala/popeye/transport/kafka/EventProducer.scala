package popeye.transport.kafka

import akka.actor.{Props, ActorLogging, Actor}
import kafka.producer._
import java.util.Properties
import popeye.transport.proto.Message.Batch
import kafka.utils.VerifiableProperties
import kafka.serializer.{DefaultEncoder, Encoder}
import com.typesafe.config.Config
import popeye.transport.ConfigUtil._
import popeye.uuid.IdGenerator
import scala.collection.JavaConversions.{asScalaBuffer, asJavaIterable}
import popeye.transport.proto.Storage.Ensemble
import kafka.producer.KeyedMessage
import akka.actor.Status.Failure

object EventProducer {
  private def composeConfig(config: Config) = {
    config.getConfig("kafka.producer")
      .withFallback(config.getConfig("kafka"))
  }

  private def producerConfig(config: Config) = {
    val local: Config = config
    val props: Properties = new Properties()
    props.putAll(local)
    props.setProperty("zookeeper.connect", local.getString("zk.cluster"))
    props.setProperty("serializer.class", classOf[EnsembleEncoder].getCanonicalName)
    props.setProperty("key.serializer.class", classOf[DefaultEncoder].getCanonicalName)
    props.setProperty("compression", "gzip")
    new ProducerConfig(props)
  }

  def props(config:Config)(implicit idGenerator: IdGenerator) = {
    val flatConfig: Config = composeConfig(config)
    Props(new EventProducer(flatConfig.getString("events.topic"), producerConfig(flatConfig), idGenerator))
  }
}

class EventProducer(topic: String,
                    config: ProducerConfig,
                    idGenerator: IdGenerator)
  extends Actor with ActorLogging {

  val producer = new Producer[Array[Byte], Ensemble](config)

  override def postStop() {
    producer.close()
    super.postStop()
  }

  def receive = {

    case PersistBatch(events) => {
      try {

        for {
          (part, list) <- events.getEventList
            .groupBy(ev => (ev.getMetric.hashCode() % 16) -> ev)

          id: Long = idGenerator.nextId()

          ensemble = Ensemble.newBuilder()
            .setBatchId(id)
            .addAllEvents(list)
            .build
        } yield {
          producer.send(
            KeyedMessage(topic, id.toString.getBytes, ensemble)
          )
          sender ! BatchPersisted(id)
        }
      } catch {
        case e: Exception => sender ! Failure(e)
          throw e
      }
    }
  }
}

case class PersistBatch(events: Batch)

case class BatchPersisted(batchId: Long)

class EnsembleEncoder(props: VerifiableProperties = null) extends Encoder[Ensemble] {
  override def toBytes(value: Ensemble): Array[Byte] = value.toByteArray
}

