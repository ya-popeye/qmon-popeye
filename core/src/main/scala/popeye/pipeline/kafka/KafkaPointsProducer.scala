package popeye.pipeline.kafka

import _root_.kafka.serializer.{Decoder, Encoder}
import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import kafka.producer._
import kafka.utils.VerifiableProperties
import popeye.pipeline._
import popeye.proto.PackedPoints
import popeye.{IdGenerator, ConfigUtil}
import scala.concurrent.{ExecutionContext, Future, Promise}

class KafkaPointsProducerConfig(config: Config)
  extends PointsDispatcherConfig(config.getConfig("producer")) {

  val topic = config.getString("topic")
}

class KafkaProducerMetrics(prefix: String, metricsRegistry: MetricRegistry)
  extends PointsDispatcherMetrics(s"$prefix.producer", metricsRegistry)

class KafkaPointsProducerWorker(kafkaClient: PopeyeKafkaProducerFactory,
                                val batcher: KafkaPointsProducer)
  extends PointsDispatcherWorkerActor {

  type Batcher = KafkaPointsProducer

  val producer = kafkaClient.newProducer(batcher.config.topic)

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _ => Restart
  }

  override def postStop() {
    super.postStop()
    producer.close()
  }

  def processBatch(batchId: Long, buffer: Seq[PackedPoints]): Unit = {
    producer.sendPacked(batchId, buffer: _*)
  }
}

class KafkaPointsProducer(producerConfig: Config,
                          val idGenerator: IdGenerator,
                          kafkaClient: PopeyeKafkaProducerFactory,
                          val metrics: KafkaProducerMetrics,
                          akkaDispatcher: Option[String])
  extends PointsDispatcherActor {

  type Config = KafkaPointsProducerConfig
  type Metrics = KafkaProducerMetrics
  val config = new KafkaPointsProducerConfig(producerConfig)

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _ ⇒ Restart
  }

  private var idx = 0

  def spawnWorker(): ActorRef = {
    idx += 1
    val workerProps = Props.apply(new KafkaPointsProducerWorker(kafkaClient, this)).withDeploy(Deploy.local)
    val propsWithDispatcher = akkaDispatcher.map(disp => workerProps.withDispatcher(disp)).getOrElse(workerProps)
    context.actorOf(
      propsWithDispatcher,
      "points-sender-" + idx)
  }
}

class KafkaPointsSink(producer: ActorRef)(implicit eCtx: ExecutionContext) extends PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
    val promise = Promise[Long]()
    KafkaPointsProducer.produce(producer, Some(promise), points)
    val pointsInPack = points.pointsCount
    promise.future map { batchId => pointsInPack.toLong}
  }
}

object KafkaPointsProducer {

  type ProducerFactory = (ProducerConfig) => Producer[Int, Array[Byte]]

  def produce(producer: ActorRef, promise: Option[Promise[Long]], points: PackedPoints) = {
    producer ! DispatcherProtocol.Pending(promise)(Seq(points))
  }

  def producerConfig(kafkaConfig: Config): ProducerConfig = {
    val producerProps = ConfigUtil.mergeProperties(kafkaConfig, "producer.config")
    producerProps.setProperty("metadata.broker.list", kafkaConfig.getString("broker.list"))
    producerProps.setProperty("key.serializer.class", classOf[KeySerialiser].getName)
    producerProps.setProperty("partitioner.class", classOf[KeyPartitioner].getName)
    new ProducerConfig(producerProps)
  }

  def props(prefix: String, config: Config, idGenerator: IdGenerator,
            kafkaClient: PopeyeKafkaProducerFactory, metricRegistry: MetricRegistry, akkaDispatcher: Option[String] = None) = {
    val metrics = new KafkaProducerMetrics(prefix, metricRegistry)
    Props.apply(new KafkaPointsProducer(
      config,
      idGenerator,
      kafkaClient,
      metrics,
      akkaDispatcher))
  }

  def defaultProducerFactory(config: ProducerConfig) = new Producer[Int, Array[Byte]](config)
}

class KeyPartitioner(props: VerifiableProperties = null) extends Partitioner[Long] {
  def partition(data: Long, numPartitions: Int): Int = (data % numPartitions).toInt
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

class KeyDeserializer(props: VerifiableProperties = null) extends Decoder[Long] {
  def fromBytes(bytes: Array[Byte]): Long = {
    if (bytes.length != 8)
      throw new IllegalArgumentException(s"Need 8 bytes, got ${bytes.length}")

    (bytes(0).toLong << 56) +
      ((bytes(1) & 255).toLong << 48) +
      ((bytes(2) & 255).toLong << 40) +
      ((bytes(3) & 255).toLong << 32) +
      ((bytes(4) & 255).toLong << 24) +
      ((bytes(5) & 255).toLong << 16) +
      ((bytes(6) & 255).toLong << 8) +
      ((bytes(7) & 255).toLong << 0)
  }
}
