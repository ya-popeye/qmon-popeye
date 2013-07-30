package popeye.transport.kafka

import akka.actor._
import kafka.producer._
import java.util.Properties
import com.typesafe.config.Config
import popeye.{IdGenerator, ConfigUtil, Instrumented}
import ConfigUtil._
import popeye.transport.proto.Storage.Ensemble
import akka.routing.FromConfig
import kafka.producer.KeyedMessage
import akka.actor.Status.Failure
import com.codahale.metrics.{Timer, MetricRegistry}
import kafka.client.ClientUtils
import scala.collection
import kafka.utils.VerifiableProperties
import scala.collection.mutable.ArrayBuffer
import popeye.transport.proto.Message.Point
import scala.collection.mutable
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.Restart
import scala.collection.immutable.Queue

case class KafkaPointProducerMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer("kafka.produce.time")
  val sendTimer = metrics.timer("kafka.send.time")
  val pointsMeter = metrics.meter("kafka.produce.points")
  val batchSizeHist = metrics.histogram("kafka.produce.batch.size")
  val batchFailedMeter = metrics.meter("kafka.produce.batch.failed")
  val batchFailedComplete = metrics.meter("kafka.produce.batch.complete")
}

case object PointSenderReady
case class ProducePack(batchId: Long, pointsCount: Long, notificationsCount: Long, started: Timer.Context)
                      (val ensembles: mutable.HashMap[Int, Ensemble.Builder],
                       val notifications: mutable.Map[ActorRef, ArrayBuffer[Long]])

class KafkaPointSender(topic: String, producerConfig: ProducerConfig, metrics: KafkaPointProducerMetrics) extends Actor with ActorLogging {

  val producer = new Producer[Nothing, Ensemble](producerConfig)


  override def preStart() {
    super.preStart()
    log.debug("Starting sender")
    context.parent ! PointSenderReady
  }

  override def postStop() {
    log.debug("Stopping sender")
    super.postStop()
    producer.close()
  }

  def receive = {
    case p@ProducePack(batchId, pointCount, ensemblesCount, started) =>
      try {
        metrics.pointsMeter.mark(p.pointsCount)
        metrics.sendTimer.time {
          producer.send(p.ensembles.map(e => new KeyedMessage(topic, e._2.build)).toArray: _*)
        }
        if (log.isDebugEnabled)
          log.debug("Sent {} ensembles with {} points", ensemblesCount, p.pointsCount)
        p.notifications foreach {
          p =>
            p._1 ! ProduceDone(p._2, batchId)
        }
        metrics.batchFailedComplete.mark(p.ensembles.size)
      } catch {
        case e: Exception => sender ! Failure(e)
          metrics.batchFailedComplete.mark(p.ensembles.size)
          p.notifications foreach {
            p =>
              p._1 ! ProduceFailed(p._2, e)
          }
          throw e
      } finally {
        started.close()
        context.parent ! PointSenderReady
      }

  }
}

class KafkaPointProducer(config: Config,
                         producerConfig: ProducerConfig,
                         idGenerator: IdGenerator,
                         metrics: KafkaPointProducerMetrics)
  extends Actor with ActorLogging {

  val topic = config.getString("kafka.points.topic")
  val partitions = ClientUtils
    .fetchTopicMetadata(
    Set(topic),
    ClientUtils.parseBrokerList(producerConfig.brokerList), producerConfig, 1
  ).topicsMetadata
    .filter(_.topic == topic)
    .head.partitionsMetadata.size

  val batchWaitTimeout: FiniteDuration = toFiniteDuration(config.getMilliseconds("kafka.produce.batch-timeout"))
  val batchSize = config.getInt("kafka.produce.batch-size")
  val senders = config.getInt("kafka.produce.senders")

  case class CorrelatedPoint(correlationId: Long, sender: ActorRef)(val events: Seq[Point])

  var pending = new ArrayBuffer[CorrelatedPoint]
  var flusher: Option[Cancellable] = None

  var workQueue = Queue[ActorRef]()

  override val supervisorStrategy =
    OneForOneStrategy(loggingEnabled = true) {
      case _ ⇒ Restart
    }

  override def preStart() {
    super.preStart()
    log.debug("Starting batcher")

    for (i <- 1 to senders) {
      context.actorOf(Props(new KafkaPointSender(topic, producerConfig, metrics)), "points-sender-" + i)
    }

    import context.dispatcher
    flusher = Some(context.system.scheduler.schedule(batchWaitTimeout, batchWaitTimeout, self, FlushPoints))
  }

  override def postStop() {
    log.debug("Stopping batcher")
    flusher foreach {
      _.cancel()
    }
    super.postStop()
  }

  def receive: Actor.Receive = {
    case FlushPoints =>
      sendPoints()

    case PointSenderReady =>
      workQueue = workQueue.enqueue(sender)
      sendPoints()

    case p@ProducePending(corr) =>
      p.data match {
        case Nil =>
        case points =>
          pending += CorrelatedPoint(p.correlationId, sender)(points)
      }
      sendPoints(batchSize)
  }

  def tryFlush() = {
    if (pending.length >= batchSize) {
      self ! FlushPoints
    }
  }

  def sendPoints(batchSize: Int = 0) = if (workQueue.size > 0 && pending.length > batchSize)
  {
    val ctx = metrics.writeTimer.timerContext()
    val batchId = idGenerator.nextId()
    val ensembles = new collection.mutable.HashMap[Int, Ensemble.Builder]
    val batch = if (batchSize > 0) {
      val parts = pending.splitAt(batchSize)
      pending = parts._2
      parts._1
    } else {
      val what = pending
      pending = new ArrayBuffer
      what
    }
    if (log.isDebugEnabled)
      log.debug("Ready to produce {} points, pending {}", batch.size, pending.size)
    val notifications = mutable.Map[ActorRef, ArrayBuffer[Long]]()
    batch.foreach {
      ev =>
        ev.events.foreach {
          event =>
            val part = Math.abs(event.getMetric.hashCode()) % partitions
            val ensemble = ensembles.getOrElseUpdate(part,
              Ensemble.newBuilder()
                .setBatchId(batchId)
                .setPartition(part)
            )
            ensemble.addPoints(event)
            val senderPointId = notifications.getOrElse(ev.sender, new ArrayBuffer)
            senderPointId += ev.correlationId
            notifications.put(ev.sender, senderPointId)
        }
    }
    var pointsCount = 0;
    val ensemblesCount = ensembles.size
    ensembles foreach {
      e =>
        metrics.batchSizeHist.update(e._2.getPointsCount)
        pointsCount += e._2.getPointsCount
    }
    workQueue.dequeue match {
      case (actor, wq) =>
        workQueue = wq
        actor ! ProducePack(batchId, pointsCount, ensemblesCount, ctx)(ensembles, notifications)
    }
    tryFlush()
  }
}

class EnsemblePartitioner(props: VerifiableProperties = null) extends Partitioner[Ensemble] {
  def partition(data: Ensemble, numPartitions: Int): Int = {
    Math.abs(data.getPartition) % numPartitions
  }
}

object KafkaPointProducer {

  def start(config: Config, idGenerator: IdGenerator)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    system.actorOf(KafkaPointProducer.props(config, idGenerator)
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
    val metrics = KafkaPointProducerMetrics(metricRegistry)
    Props(new KafkaPointProducer(
      config,
      producerConfig(config),
      idGenerator,
      metrics))
  }
}


