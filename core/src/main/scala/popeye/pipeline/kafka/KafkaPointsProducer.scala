package popeye.pipeline.kafka

import _root_.kafka.serializer.{Decoder, Encoder}
import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import kafka.producer._
import kafka.utils.VerifiableProperties
import popeye.pipeline._
import popeye.proto.{Message, PackedPoints}
import popeye.{IdGenerator, ConfigUtil}
import scala.concurrent.{ExecutionContext, Future, Promise}

class KafkaPointsProducerConfig(config: Config)
  extends PointsDispatcherConfig(config.getConfig("producer")) {

  val topic = config.getString("topic")
}

class KafkaProducerMetrics(prefix: String, metricsRegistry: MetricRegistry)
  extends PointsDispatcherMetrics(s"$prefix.producer", metricsRegistry)

class KafkaPointsProducerWorker(kafkaClient: PointsSinkFactory,
                                val batcher: KafkaPointsProducer)
  extends PointsDispatcherWorkerActor {

  type Batcher = KafkaPointsProducer

  val producer = kafkaClient.newSender(batcher.config.topic)

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

class KafkaPointsProducer(val config: KafkaPointsProducerConfig,
                          val idGenerator: IdGenerator,
                          kafkaClient: PointsSinkFactory,
                          val metrics: KafkaProducerMetrics,
                          akkaDispatcher: Option[String])
  extends PointsDispatcherActor {

  type Config = KafkaPointsProducerConfig
  type Metrics = KafkaProducerMetrics

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _ ⇒ Restart
  }

  private var idx = 0

  def spawnWorker(): ActorRef = {
    idx += 1
    val workerProps = Props.apply(new KafkaPointsProducerWorker(kafkaClient, this)).withDeploy(Deploy.local)
    val propsWithDispatcher = akkaDispatcher.fold(workerProps)(disp => workerProps.withDispatcher(disp))
    context.actorOf(
      propsWithDispatcher,
      "points-sender-" + idx)
  }
}

object KafkaPointsProducer {

  type ProducerFactory = (ProducerConfig) => Producer[Int, Array[Byte]]

  def producePacked(producer: ActorRef, promise: Option[Promise[Long]], points: PackedPoints*) = {
    producer ! DispatcherProtocol.Pending(promise)(points)
  }

  def producePoints(producer: ActorRef, promise: Option[Promise[Long]], points: Message.Point*) = {
    producer ! DispatcherProtocol.Pending(promise)(PackedPoints(points))
  }

  def producerConfig(kafkaConfig: Config): ProducerConfig = {
    val producerProps = ConfigUtil.mergeProperties(kafkaConfig, "producer.config")
    producerProps.setProperty("metadata.broker.list", kafkaConfig.getString("broker.list"))
    producerProps.setProperty("key.serializer.class", classOf[KeySerialiser].getName)
    producerProps.setProperty("partitioner.class", classOf[KeyPartitioner].getName)
    new ProducerConfig(producerProps)
  }

  def props(prefix: String, config: KafkaPointsProducerConfig, idGenerator: IdGenerator,
            kafkaClient: PointsSinkFactory, metricRegistry: MetricRegistry,
            akkaDispatcher: Option[String] = None) = {
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

class KeyPartitioner(props: VerifiableProperties = null) extends Partitioner {

  def partition(data: Any, numPartitions: Int): Int = {
    import scala.util.hashing.MurmurHash3
    val hash = MurmurHash3.listHash(List(data.asInstanceOf[Long]), 0)
    math.abs(hash) % numPartitions
  }
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
