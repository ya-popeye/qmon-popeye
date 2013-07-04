package popeye.transport.kafka

import akka.actor.{Props, ActorLogging, Actor}
import kafka.producer.{Partitioner, KeyedMessage, ProducerConfig, Producer}
import java.util.Properties
import popeye.transport.proto.Message.Event
import kafka.utils.VerifiableProperties
import kafka.serializer.Encoder
import akka.actor.Status.Failure
import com.typesafe.config.Config
import popeye.transport.ConfigUtil._

object EventProducer {
  def props() = {
    Props[EventProducer]()
  }

  def fromConfig(config: Config): Props = {
    val localConfig = config.getConfig("kafka.producer")
      .withFallback(config.getConfig("kafka"))
    Props(new EventProducer(localConfig))
  }
}

class EventProducer(kafkaConfig: Config) extends Actor with ActorLogging {

  val topic = kafkaConfig.getString("events.topic")
  var kafka: Option[Producer[Nothing, Event]] = Some(new Producer[Nothing, Event](producerConfig()))

  def producerConfig() = {
    val props: Properties = new Properties()
    props.putAll(kafkaConfig)
    props.setProperty("zookeeper.connect", kafkaConfig.getString("zk.cluster"))
    props.setProperty("partitioner.class", classOf[EventHashPartitioner].getCanonicalName)
    props.setProperty("serializer.class", classOf[EventEncoder].getCanonicalName)
    new ProducerConfig(props)
  }

  override def postStop() = {
    log.debug("Stopping EventProducer")
    kafka foreach {
      p => p.close()
    }
    super.postStop()
  }

  def receive = {
    case PersistOnQueue(events) => {
      try {
        kafka.get.send(
          events.map({
            e: Event => new KeyedMessage(topic, e)
          }): _*
        )
        sender ! EventPersisted(0)
      } catch {
        case e: Exception => sender ! Failure(e)
          throw e
      }
    }
  }
}

case class PersistOnQueue(events: Seq[Event])

case class EventPersisted(offset: Long)

class EventHashPartitioner(props: VerifiableProperties = null) extends Partitioner[Event] {
  def partition(data: Event, numPartitions: Int): Int = {
    data.getMetric.hashCode() % numPartitions
  }
}

class EventEncoder(props: VerifiableProperties = null) extends Encoder[Event] {
  override def toBytes(value: Event): Array[Byte] = value.toByteArray
}

