package popeye.transport.kafka

import akka.actor._
import kafka.producer._
import java.util.Properties
import com.typesafe.config.Config
import popeye.transport.ConfigUtil._
import popeye.uuid.IdGenerator
import popeye.transport.proto.Storage.Ensemble
import akka.routing.FromConfig
import kafka.producer.KeyedMessage
import akka.actor.Status.Failure
import com.codahale.metrics.MetricRegistry
import popeye.Instrumented
import kafka.client.ClientUtils
import scala.collection
import kafka.utils.VerifiableProperties
import scala.collection.mutable.ArrayBuffer
import popeye.transport.proto.Message.Event
import scala.collection.mutable
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.Restart

case class ProducePack(sender: ActorRef, req: ProducePending)

case class KafkaEventProducerMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer("kafka.produce.time")
  val pointsMeter = metrics.meter("kafka.produce.points")
  val batchSizeHist = metrics.histogram("kafka.produce.batch.size")
  val batchFailedMeter = metrics.meter("kafka.produce.batch.failed")
  val batchFailedComplete = metrics.meter("kafka.produce.batch.complete")
}

class KafkaEventProducer(config: Config,
                         producerConfig: ProducerConfig,
                         idGenerator: IdGenerator,
                         metrics: KafkaEventProducerMetrics)
  extends Actor with ActorLogging {

  val topic = config.getString("kafka.points.topic")
  val producer = new Producer[Nothing, Ensemble](producerConfig)
  val partitions = ClientUtils
    .fetchTopicMetadata(
    Set(topic),
    ClientUtils.parseBrokerList(producerConfig.brokerList), producerConfig, 1
  ).topicsMetadata
    .filter(_.topic == topic)
    .head.partitionsMetadata.size

  val batchSize = config.getInt("kafka.produce.batch-size")

  case class CorrelatedPoint(correlationId: Long, sender: ActorRef)(val events: Seq[Event])

  var pending = new ArrayBuffer[CorrelatedPoint]

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = (5 minutes), loggingEnabled = true) {
      case _ ⇒ Restart
    }

  def receive: Actor.Receive = {
    case p@ProducePending(corr) =>
      p.data match {
        case Nil =>
        case points =>
          pending += CorrelatedPoint(p.correlationId, sender)(points)
      }
      if (pending.length >= batchSize) {
        sendPoints()
      }
      if (pending.length > 0)
        self ! ProducePending(0)(Nil)
  }

  def sendPoints() = {
    val ctx = metrics.writeTimer.timerContext()
    val batchId = idGenerator.nextId()
    val ensembles = new collection.mutable.HashMap[Int, Ensemble.Builder]
    val parts = pending.splitAt(batchSize)
    val batch = parts._1
    pending = parts._2
    if (log.isDebugEnabled)
      log.debug("Ready to produce {} points, pending {}", batch.size, pending.size)
    val notifications = mutable.Map[ActorRef, ArrayBuffer[Long]]()
    batch.foreach {
      ev =>
        ev.events.foreach {
          event =>
            val part = event.getMetric.hashCode() % partitions
            val ensemble = ensembles.getOrElseUpdate(part,
              Ensemble.newBuilder()
                .setBatchId(batchId)
                .setPartition(part)
            )
            ensemble.addEvents(event)
            val senderPointId = notifications.getOrElse(ev.sender, new ArrayBuffer)
            senderPointId += ev.correlationId
            notifications.put(ev.sender, senderPointId)
        }
    }
    try {
      var pointsCount = 0;
      val ensemblesCount = ensembles.size
      ensembles foreach { e =>
        metrics.batchSizeHist.update(e._2.getEventsCount)
        pointsCount += e._2.getEventsCount
      }
      if (log.isDebugEnabled)
        log.debug("Producing {} points, pending {}", batch.size, pending.size)
      metrics.pointsMeter.mark(pointsCount)
      producer.send(ensembles.map(e => new KeyedMessage(topic, e._2.build)).toArray: _*)
      if (log.isDebugEnabled)
        log.debug("Produced {} ensembles with {} points, pending {}", ensemblesCount, pointsCount, pending.size)
      notifications foreach { p =>
        p._1 ! ProduceDone(p._2, batchId)
      }
      metrics.batchFailedComplete.mark(ensembles.size)
    } catch {
      case e: Exception => sender ! Failure(e)
        metrics.batchFailedComplete.mark(ensembles.size)
        notifications foreach { p =>
          p._1 ! ProduceFailed(p._2, e)
        }
        throw e
    } finally {
      ctx.close()
    }
  }

  override def preStart() {
    log.debug("Starting producer")
    super.preStart()
  }

  override def postStop() {
    log.debug("Stopping producer")
    if (pending.size > 0)
      sendPoints()
    super.postStop()
    producer.close()
  }

}

class EnsemblePartitioner(props: VerifiableProperties = null) extends Partitioner[Ensemble] {
  def partition(data: Ensemble, numPartitions: Int): Int = {
    data.getPartition
  }
}

object KafkaEventProducer {

  def start(config: Config, idGenerator: IdGenerator)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    system.actorOf(KafkaEventProducer.props(config, idGenerator)
      .withRouter(FromConfig())
      .withDispatcher("kafka.produce.dispatcher"), "kafka-producer")
  }

  def producerConfig(globalConfig: Config): ProducerConfig = {
    val config: Config = globalConfig.getConfig("kafka.producer")
    val producerProps: Properties = config
    producerProps.setProperty("serializer.class", classOf[EnsembleEncoder].getName)
    producerProps.setProperty("partitioner.class", classOf[EnsemblePartitioner].getName)
    producerProps.setProperty("producer.type", "sync")
    new ProducerConfig(producerProps)
  }

  def props(config: Config, idGenerator: IdGenerator)(implicit metricRegistry: MetricRegistry) = {
    val metrics = KafkaEventProducerMetrics(metricRegistry)
    Props(new KafkaEventProducer(
      config,
      producerConfig(config),
      idGenerator,
      metrics))
  }
}


