package popeye.transport.kafka

import akka.actor._
import kafka.producer._
import java.util.Properties
import com.typesafe.config.Config
import popeye.{Logging, IdGenerator, ConfigUtil, Instrumented}
import ConfigUtil._
import akka.routing.FromConfig
import com.codahale.metrics.{Timer, MetricRegistry}
import kafka.client.ClientUtils
import kafka.utils.VerifiableProperties
import popeye.transport.proto.Message.Point
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.Restart
import popeye.transport.proto.{PointsQueue, PackedPoints}
import scala.annotation.tailrec
import scala.concurrent.Promise
import akka.actor.Status.Failure
import scala.Some
import akka.actor.OneForOneStrategy
import kafka.serializer.Encoder


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
                        (val buffer: PointsQueue.PartitionBuffer, val promises: Seq[Promise[Long]])

}

class KafkaPointSender(topic: String,
                       producerConfig: ProducerConfig,
                       metrics: KafkaPointProducerMetrics,
                       batcher: KafkaPointProducer)
  extends Actor with Logging {

  import KafkaPointProducerProtocol._

  val producer = new Producer[Int, Array[Byte]](producerConfig)

  override def preStart() {
    super.preStart()
    debug("Starting sender")
    batcher.addWorker(self)
  }

  override def postStop() {
    debug("Stopping sender")
    super.postStop()
    producer.close()
  }

  def receive = {
    case p@ProducePack(batchId, started) =>
      val sendctx = metrics.sendTimer.timerContext()

      try {
        val message = new KeyedMessage(topic, p.buffer.partitionId, PackedPoints.prependBatchId(batchId, p.buffer.buffer))
        producer.send(message)
        debug(s"Sent batch ${p.batchId} to partition ${p.buffer.partitionId}")
        metrics.pointsMeter.mark(p.buffer.points)
        metrics.batchCompleteMeter.mark()
        withDebug {
          p.promises.foreach {
            promise =>
              debug(s"${self.path} got promise $promise for SUCCESS batch ${p.batchId}")
          }
        }
        p.promises
          .filter(!_.isCompleted) // we should check, that pormise not complete before
          .foreach(_.success(batchId))
      } catch {
        case e: Exception => sender ! Failure(e)
          metrics.batchFailedMeter.mark()
          withDebug {
            p.promises.foreach {
              promise =>
                log.debug(s"${self.path} got promise for FAILURE $promise for batch ${p.batchId}")
            }
          }
          p.promises
            .filter(!_.isCompleted) // we should check, that pormise not complete before
            .foreach(_.failure(e))
          throw e
      } finally {
        val sended = sendctx.stop.nano
        val elapsed = started.stop().nano
        log.debug(s"batch ${p.batchId} sent in ${sended.toMillis}ms at total ${elapsed.toMillis}ms")
        batcher.addWorker(self)
        sender ! WorkDone(batchId)
      }
  }
}


class KafkaPointProducer(config: Config,
                         producerConfig: ProducerConfig,
                         idGenerator: IdGenerator,
                         producerFactory: KafkaPointProducer.ProducerFactory,
                         val metrics: KafkaPointProducerMetrics)
  extends Actor with Logging {

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
  val pendingPoints = new PointsQueue(partitions, minMessageBytes, maxMessageBytes)

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

    for (i <- 0 until senders) {
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

  @tailrec
  private def flushPoints(ignoreMinSize: Boolean = false): Unit = {
    if (workQueue.isEmpty)
      return
    workQueue.headOption() match {
      case Some(worker: ActorRef) =>
        val batchId = idGenerator.nextId()
        val (data, promises) = pendingPoints.consume(ignoreMinSize)
        if (!data.isEmpty) {
          worker ! ProducePack(batchId, metrics.writeTimer.timerContext())(data.get, promises)
          log.debug(s"Sending ${data.foldLeft(0)({
            (a, b) => a + b.buffer.length
          })} bytes, will trigger ${promises.size} promises")
          flushPoints(ignoreMinSize)
        } else {
          workQueue.add(worker)
        }
      case None =>
        return
    }
  }

}

object KafkaPointProducer {

  type ProducerFactory = (ProducerConfig) => Producer[Int, Array[Byte]]

  def start(config: Config, idGenerator: IdGenerator)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    system.actorOf(KafkaPointProducer.props(config, idGenerator)
      .withRouter(FromConfig())
      .withDispatcher("kafka.produce.dispatcher"), "kafka-producer")
  }

  def producerConfig(globalConfig: Config): ProducerConfig = {
    val config: Config = globalConfig.getConfig("kafka.producer")
    val producerProps: Properties = config
    producerProps.setProperty("producer.type", "sync")
    producerProps.setProperty("key.serializer.class", classOf[KeySerialiser].getName)
    producerProps.setProperty("partitioner.class", classOf[KeyPartitioner].getName)
    new ProducerConfig(producerProps)
  }

  def props(config: Config, idGenerator: IdGenerator,
            producerFactory: ProducerFactory = defaultProducerFactory)
           (implicit metricRegistry: MetricRegistry) = {
    val metrics = KafkaPointProducerMetrics(metricRegistry)
    Props(new KafkaPointProducer(
      config,
      producerConfig(config),
      idGenerator,
      producerFactory,
      metrics))
  }

  def defaultProducerFactory(config: ProducerConfig) = new Producer[Int, Array[Byte]](config)
}

class KeySerialiser(props: VerifiableProperties = null) extends Encoder[Int] {
  def toBytes(p1: Int): Array[Byte] = {
    val conv = new Array[Byte](4)
    var input = p1
    conv(3) = (input & 0xff).toByte;
    input >>= 8;
    conv(2) = (input & 0xff).toByte;
    input >>= 8;
    conv(1) = (input & 0xff).toByte;
    input >>= 8;
    conv(0) = input.toByte;
    conv
  }

}

class KeyPartitioner(props: VerifiableProperties = null) extends Partitioner[Int] {
  def partition(data: Int, numPartitions: Int): Int = data % numPartitions
}


