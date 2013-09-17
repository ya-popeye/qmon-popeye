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
import scala.collection.JavaConversions.iterableAsScalaIterable
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
  val writeTimer = metrics.timer("kafka.producer.wall-time")
  val sendTimer = metrics.timer("kafka.producer.send-time")
  val pointsMeter = metrics.meter("kafka.producer.points")
  val batchFailedMeter = metrics.meter("kafka.producer.batch-failed")
  val batchCompleteMeter = metrics.meter("kafka.producer.batch-complete")
}

private object KafkaPointProducerProtocol {

  case class WorkDone(batchId: Long)

  case class CorrelatedPoint(correlationId: Long, sender: ActorRef)(val points: Seq[Point])

  case class ProducePack(batchId: Long, started: Timer.Context)
                        (val buffer: PointsQueue.PartitionBuffer, val promises: Seq[Promise[Long]])

}

class KafkaPointSender(topic: String,
                       kafkaClient: PopeyeKafkaProducerFactory,
                       metrics: KafkaPointProducerMetrics,
                       batcher: KafkaPointProducer)
  extends Actor with Logging {

  import KafkaPointProducerProtocol._

  val producer = kafkaClient.newProducer()

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
        val message = new KeyedMessage(topic, batchId, PackedPoints.prependBatchId(batchId, p.buffer.buffer))
        producer.send(message)
        debug(s"Sent batch ${p.batchId}")
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
                         idGenerator: IdGenerator,
                         kafkaClient: PopeyeKafkaProducerFactory,
                         val metrics: KafkaPointProducerMetrics)
  extends Actor with Logging {

  import KafkaPointProducerProtocol._

  val topic = config.getString("kafka.points.topic")

  val batchWaitTimeout: FiniteDuration = toFiniteDuration(config.getMilliseconds("kafka.producer.batch-timeout"))
  val maxQueued = config.getInt("kafka.producer.max-queued")
  val senders = config.getInt("kafka.producer.senders")
  val minMessageBytes = config.getInt("kafka.producer.message.min-bytes")
  val maxMessageBytes = config.getInt("kafka.producer.message.max-bytes")
  val pendingPoints = new PointsQueue(minMessageBytes, maxMessageBytes)

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
        Props.apply(new KafkaPointSender(topic, kafkaClient, metrics, this)).withDeploy(Deploy.local),
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

  def start(config: Config, idGenerator: IdGenerator)
           (implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    val kafkaClient = new PopeyeKafkaProducerFactoryImpl(producerConfig(config))
    system.actorOf(KafkaPointProducer.props(config, idGenerator, kafkaClient)
      .withRouter(FromConfig())
      .withDispatcher("kafka.producer.dispatcher"), "kafka-producer")
  }

  def producerConfig(globalConfig: Config): ProducerConfig = {
    val producerProps = ConfigUtil.mergeProperties(globalConfig, "kafka.producer.config")
    producerProps.setProperty("metadata.broker.list", globalConfig.getString("kafka.broker.list"))
    producerProps.setProperty("key.serializer.class", classOf[KeySerialiser].getName)
    producerProps.setProperty("partitioner.class", classOf[KeyPartitioner].getName)
    new ProducerConfig(producerProps)
  }

  def props(config: Config, idGenerator: IdGenerator, kafkaClient: PopeyeKafkaProducerFactory)
           (implicit metricRegistry: MetricRegistry) = {
    val metrics = KafkaPointProducerMetrics(metricRegistry)
    Props.apply(new KafkaPointProducer(
      config,
      idGenerator,
      kafkaClient,
      metrics))
  }

  def defaultProducerFactory(config: ProducerConfig) = new Producer[Int, Array[Byte]](config)
}

class KeySerialiser(props: VerifiableProperties = null) extends Encoder[Long] {
  def toBytes(p1: Long): Array[Byte] = {
    val conv = new Array[Byte](8)
    var input = p1
    conv(7) = (input & 0xff).toByte
    input >>= 8
    conv(6) = (input & 0xff).toByte
    input >>= 8
    conv(5) = (input & 0xff).toByte
    input >>= 8
    conv(4) = (input & 0xff).toByte
    input >>= 8
    conv(3) = (input & 0xff).toByte
    input >>= 8
    conv(2) = (input & 0xff).toByte
    input >>= 8
    conv(1) = (input & 0xff).toByte
    input >>= 8
    conv(0) = input.toByte
    conv
  }

}

class KeyPartitioner(props: VerifiableProperties = null) extends Partitioner[Long] {
  def partition(data: Long, numPartitions: Int): Int = (data % numPartitions).toInt
}


