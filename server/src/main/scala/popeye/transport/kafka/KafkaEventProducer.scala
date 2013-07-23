package popeye.transport.kafka

import akka.actor._
import kafka.producer._
import java.util.Properties
import com.typesafe.config.Config
import popeye.transport.ConfigUtil._
import popeye.uuid.IdGenerator
import scala.collection.JavaConversions.{asScalaBuffer, asJavaIterable}
import scala.concurrent.duration._
import popeye.transport.proto.Storage.Ensemble
import akka.routing.FromConfig
import kafka.producer.KeyedMessage
import akka.actor.Status.Failure
import com.codahale.metrics.MetricRegistry
import popeye.{Instrumented, BufferedFSM}
import popeye.BufferedFSM.Todo
import kafka.client.ClientUtils
import scala.collection
import kafka.utils.VerifiableProperties
import scala.concurrent.duration.FiniteDuration

case class ProducePack(sender: ActorRef, req: ProducePending)

case class KafkaEventProducerMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer("kafka.produce.time")
  val batchSizeHist = metrics.histogram("kafka.produce.batch.size")
  val batchFailedMeter = metrics.meter("kafka.produce.batch.failed")
  val batchFailedComplete = metrics.meter("kafka.produce.batch.complete")
}

class KafkaEventProducer(config: Config,
                         producerConfig: ProducerConfig,
                         idGenerator: IdGenerator,
                          metrics: KafkaEventProducerMetrics)
  extends BufferedFSM[ProducePack] with ActorLogging {

  val topic = config.getString("kafka.events.topic")
  val producer = new Producer[Nothing, Ensemble](producerConfig)
  val partitions = ClientUtils.fetchTopicMetadata(Set(topic), ClientUtils.parseBrokerList(producerConfig.brokerList), producerConfig, 1)
    .topicsMetadata
    .filter(_.topic == topic).head.partitionsMetadata.size

  def timeout: FiniteDuration = new FiniteDuration(config.getMilliseconds("kafka.producer.flush.tick"), MILLISECONDS)

  def flushEntitiesCount: Int = config.getInt("kafka.producer.flush.events")

  override def consumeCollected(todo: Todo[ProducePack]) = {
    val ctx = metrics.writeTimer.timerContext()
    val batchId = idGenerator.nextId()
    val ensembles = new collection.mutable.HashMap[Int, Ensemble.Builder]
    for (
      pp <- todo.queue;
      (part, list) <- pp.req.data.getEventList
        .groupBy(ev => ev.getMetric.hashCode() % partitions)
    ) {
      val ensemble = ensembles.getOrElseUpdate(part,
        Ensemble.newBuilder()
        .setBatchId(batchId)
        .setPartition(part)
      )
      ensemble.addAllEvents(list)
    }
    try {
      ensembles foreach {e => metrics.batchSizeHist.update(e._2.getEventsCount)}
      producer.send(ensembles.map(e => new KeyedMessage(topic, e._2.build)).toArray:_*)
      todo.queue foreach {
        p =>
          p.sender ! ProduceDone(p.req.correlationId, batchId)
      }
      metrics.batchFailedComplete.mark(ensembles.size)
    } catch {
      case e: Exception => sender ! Failure(e)
        metrics.batchFailedComplete.mark(todo.queue.size)
        todo.queue foreach {
          p =>
            p.sender ! ProduceFailed(p.req.correlationId, e)
        }
        throw e
    } finally {
      ctx.close()
    }
  }

  val handleMessage: TodoFunction = {
    case Event(p@ProducePending(events, correlation), todo) =>
      val t = todo.copy(entityCnt = todo.entityCnt + events.getEventCount(), queue = todo.queue :+ ProducePack(sender, p))
      if (log.isDebugEnabled)
        log.debug("Queued {} todo.queue={}", correlation, t.queue.size)
      t
  }

  initialize()

  override def preStart() {
    log.debug("Starting producer")
    super.preStart()
  }

  override def postStop() {
    log.debug("Stopping producer")
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
      .withRouter(FromConfig()), "kafka-producer")
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


