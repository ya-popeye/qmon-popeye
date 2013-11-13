package popeye.pipeline.kafka

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.routing.FromConfig
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import kafka.producer._
import kafka.serializer.Encoder
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

  type Batch = Seq[PackedPoints]
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
    producer.sendPacked(batchId, buffer :_*)
  }
}

class KafkaPointsProducer(producerConfig: Config,
                          val idGenerator: IdGenerator,
                          kafkaClient: PopeyeKafkaProducerFactory,
                          val metrics: KafkaProducerMetrics)
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
    context.actorOf(
      Props.apply(new KafkaPointsProducerWorker(kafkaClient, this)).withDeploy(Deploy.local),
      "points-sender-" + idx)
  }
}

class KafkaPointsSink(producer: ActorRef)(implicit eCtx: ExecutionContext) extends PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
    val promise = Promise[Long]()
    KafkaPointsProducer.produce(producer, Some(promise), points)
    val pointsInPack = points.pointsCount
    promise.future map { batchId => pointsInPack.toLong }
  }
}

object KafkaPointsProducer {

  type ProducerFactory = (ProducerConfig) => Producer[Int, Array[Byte]]

  def produce(producer: ActorRef, promise: Option[Promise[Long]], points: PackedPoints) = {
    producer ! DispatcherProtocol.Pending(promise)(Seq(points))
  }

  def start(name: String, config: Config, idGenerator: IdGenerator)
           (implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    val myConf = config.getConfig(name).withFallback(config.getConfig("kafka"))
    val kafkaClient = new PopeyeKafkaProducerFactoryImpl(producerConfig(myConf))
    system.actorOf(KafkaPointsProducer.props(name, config, idGenerator, kafkaClient)
      .withRouter(FromConfig())
      .withDispatcher(s"$name.producer.dispatcher"), s"$name-producer")
  }

  def producerConfig(kafkaConfig: Config): ProducerConfig = {
    val producerProps = ConfigUtil.mergeProperties(kafkaConfig, "producer.config")
    producerProps.setProperty("metadata.broker.list", kafkaConfig.getString("broker.list"))
    producerProps.setProperty("key.serializer.class", classOf[KeySerialiser].getName)
    producerProps.setProperty("partitioner.class", classOf[KeyPartitioner].getName)
    new ProducerConfig(producerProps)
  }

  def props(metricsName: String, config: Config, idGenerator: IdGenerator, kafkaClient: PopeyeKafkaProducerFactory)
           (implicit metricRegistry: MetricRegistry) = {
    val metrics = new KafkaProducerMetrics(metricsName, metricRegistry)
    Props.apply(new KafkaPointsProducer(
      config,
      idGenerator,
      kafkaClient,
      metrics))
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
