package popeye.transport.kafka

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.routing.FromConfig
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import kafka.producer._
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import popeye.pipeline._
import popeye.transport.proto.PackedPoints
import popeye.{IdGenerator, ConfigUtil}
import scala.concurrent.{ExecutionContext, Future, Promise}

class KafkaPointsProducer(producerConfig: Config,
                          val idGenerator: IdGenerator,
                          kafkaClient: PopeyeKafkaProducerFactory,
                          val metrics: KafkaProducerMetrics)
  extends PointsDispatcherActor {

  type Config = KafkaProducerConfig
  type Metrics = KafkaProducerMetrics
  val config = new KafkaProducerConfig(producerConfig)

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _ ⇒ Restart
  }

  private var idx = 0

  def spawnWorker(): ActorRef = {
    idx += 1
    context.actorOf(
      Props.apply(new KafkaPointsWorker(kafkaClient, this)).withDeploy(Deploy.local),
      "points-sender-" + idx)
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

