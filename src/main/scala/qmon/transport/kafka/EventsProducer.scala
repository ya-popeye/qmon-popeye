package qmon.transport.kafka

import akka.actor.{Props, ActorLogging, Actor}
import kafka.producer.{Partitioner, KeyedMessage, ProducerConfig, Producer}
import java.util.Properties
import qmon.transport.proto.Message.Event
import kafka.utils.VerifiableProperties
import kafka.serializer.Encoder
import akka.actor.Status.Failure

object EventsProducerActor {
  def props() = {
    Props[EventsProducerActor]()
  }
}

class EventsProducerActor(topic: String, kafkaConfig: Properties) extends Actor with ActorLogging {

  var kafka: Option[Producer[Nothing, Event]] = Some(new Producer[Nothing, Event](producerConfig()))

  def producerConfig() = {
    val props: Properties = new Properties()
    props.putAll(kafkaConfig)
    props.setProperty("partitioner.class", classOf[EventHashPartitioner].getCanonicalName)
    props.setProperty("serializer.class", classOf[EventEncoder].getCanonicalName)
    new ProducerConfig(props)
  }

  override def postStop() {
    kafka foreach {
      p => p.close()
    }
    super.postStop()
  }

  def receive = {
    case Queue(events) => {
      try {
        kafka.get.send(
          events.map({
            e: Event => new KeyedMessage("qmon-stream", e)
          }): _*
        )
        sender ! Sent()
      } catch {
        case e : Exception => sender ! Failure(e)
        throw e
      }
    }
  }
}

case class Queue(events: Seq[Event])
case class Sent()

class EventHashPartitioner(props: VerifiableProperties = null) extends Partitioner[Event] {
  def partition(data: Event, numPartitions: Int): Int = {
    data.getMetric.hashCode() % numPartitions
  }
}

class EventEncoder(props: VerifiableProperties = null) extends Encoder[Event] {
  override def toBytes(value: Event): Array[Byte] = value.toByteArray
}
