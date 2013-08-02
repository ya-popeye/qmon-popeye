package popeye.transport.kafka

import akka.actor._
import kafka.producer._
import java.util.Properties
import com.typesafe.config.Config
import popeye.{IdGenerator, ConfigUtil, Instrumented}
import ConfigUtil._
import akka.routing.FromConfig
import com.codahale.metrics.{Timer, MetricRegistry}
import kafka.client.ClientUtils
import kafka.utils.VerifiableProperties
import popeye.transport.proto.Message.Point
import scala.collection.mutable
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.Restart
import java.io.ByteArrayOutputStream
import popeye.transport.proto.{PackedPoints, PackedPointsIndex, PackedPointsBuffer}
import com.google.protobuf.{CodedOutputStream, CodedInputStream}
import scala.annotation.tailrec
import scala.concurrent.Promise
import akka.actor.Status.Failure
import scala.Some
import scala.util.Random
import akka.actor.OneForOneStrategy
import java.util


case class KafkaPointProducerMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer("kafka.produce.time")
  val sendTimer = metrics.timer("kafka.send.time")
  val pointsMeter = metrics.meter("kafka.produce.points")
  val batchFailedMeter = metrics.meter("kafka.produce.batch.failed")
  val batchCompleteMeter = metrics.meter("kafka.produce.batch.complete")
}

private object KafkaPointProducerProtocol {

  case class WorkDone(batchId: Long)

  case class CorrelatedPoint(correlationId: Long, sender: ActorRef)(val points: Seq[Point])

  case class ProducePack(batchId: Long, started: Timer.Context)
                        (val batch: Seq[PendingPoints.PartitionBuffer], val promises: Seq[Promise[Long]])

}

class KafkaPointSender(topic: String, producerConfig: ProducerConfig, metrics: KafkaPointProducerMetrics, batcher: KafkaPointProducer) extends Actor with ActorLogging {

  import KafkaPointProducerProtocol._

  val producer = new Producer[Int, Array[Byte]](producerConfig)


  override def preStart() {
    super.preStart()
    log.debug("Starting sender")
    batcher.addWorker(self)
  }

  override def postStop() {
    log.debug("Stopping sender")
    super.postStop()
    producer.close()
  }

  def receive = {
    case p@ProducePack(batchId, started) =>
      val sendctx = metrics.sendTimer.timerContext()

      val messages: Seq[KeyedMessage[Int, Array[Byte]]] = p.batch.map {
        pb =>
          metrics.pointsMeter.mark(pb.points)
          new KeyedMessage(topic, pb.partitionId, PackedPoints.prependBatchId(batchId, pb.buffer))
      }

      try {
        producer.send(messages: _*)
        metrics.batchCompleteMeter.mark
        p.promises foreach (_.success(batchId))
      } catch {
        case e: Exception => sender ! Failure(e)
          metrics.batchFailedMeter.mark
          p.promises foreach (_.failure(e))
          throw e
      } finally {
        val sended = sendctx.stop.nano
        val elapsed = started.stop().nano
        if (log.isDebugEnabled)
          log.debug("batch {} sent in {}ms at total {}ms", p.batchId, sended.toMillis, elapsed.toMillis)
        batcher.addWorker(self)
        sender ! WorkDone(batchId)
      }
  }
}


class KafkaPointProducer(config: Config,
                         producerConfig: ProducerConfig,
                         idGenerator: IdGenerator,
                         val metrics: KafkaPointProducerMetrics)
  extends Actor with ActorLogging {

  import KafkaPointProducerProtocol._

  val topic = config.getString("kafka.points.topic")
  val partitions = ClientUtils
    .fetchTopicMetadata(
    Set(topic),
    ClientUtils.parseBrokerList(producerConfig.brokerList), producerConfig, 1
  ).topicsMetadata
    .filter(_.topic == topic)
    .head.partitionsMetadata.size

  val batchWaitTimeout: FiniteDuration = toFiniteDuration(config.getMilliseconds("kafka.produce.batch-timeout"))
  val maxQueued = config.getInt("kafka.produce.max-queued")
  val senders = config.getInt("kafka.produce.senders")
  val minMessageBytes = config.getInt("kafka.produce.message.min-bytes")
  val maxMessageBytes = config.getInt("kafka.produce.message.max-bytes")
  val pendingPoints = new PendingPoints(partitions, minMessageBytes, maxMessageBytes)

  var flusher: Option[Cancellable] = None

  var workQueue = new AtomicList[ActorRef]()

  override val supervisorStrategy =
    OneForOneStrategy(loggingEnabled = true) {
      case _ ⇒ Restart
    }

  def addWorker(worker: ActorRef) {
    workQueue.add(worker)
  }

  override def preStart() {
    super.preStart()
    log.debug("Starting batcher")

    for (i <- 0 to senders) {
      context.actorOf(
        Props(new KafkaPointSender(topic, producerConfig, metrics, this)).withDeploy(Deploy.local),
        "points-sender-" + i)
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
      flushPoints(ignoreMinSize = true)

    case WorkDone(_) =>
      flushPoints()

    case p@ProducePending(promise) =>
      pendingPoints.addPending(p.data, p.batchIdPromise)
      flushPoints()
  }

  private def flushPoints(ignoreMinSize: Boolean = false): Unit = {
    while (!workQueue.isEmpty) {
      workQueue.headOption() match {
        case Some(worker: ActorRef) =>

          val batchId = idGenerator.nextId()
          val (data, promises) = pendingPoints.consume()
          if (!data.isEmpty)
            worker ! ProducePack(batchId, metrics.writeTimer.timerContext())(data, promises)

        case None =>
          return
      }
    }
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


