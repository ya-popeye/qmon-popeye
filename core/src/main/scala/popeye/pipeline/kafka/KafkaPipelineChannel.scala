package popeye.pipeline.kafka

import _root_.kafka.consumer.{ConsumerConfig, Consumer}
import akka.actor.ActorRef
import com.typesafe.config.Config
import popeye.Instrumented
import popeye.pipeline._
import popeye.proto.PackedPoints
import scala.concurrent.Promise
import akka.routing.FromConfig
import com.codahale.metrics.MetricRegistry


/**
 * @author Andrey Stepachev
 */
class KafkaPipelineChannel(val config: KafkaPipelineChannelConfig, val context: PipelineContext)
  extends PipelineChannel {

  val readerMetrics = new KafkaReaderMetrics(metrics)
  var producer: Option[ActorRef] = None
  var consumerId: Int = 0

  def newWriter(): PipelineChannelWriter = {
    if (producer.isEmpty)
      producer = Some(startProducer())
    new PipelineChannelWriter {
      def write(promise: Option[Promise[Long]], points: PackedPoints): Unit = {
        KafkaPointsProducer.producePacked(producer.get, promise, points)
      }
    }
  }

  private def startProducer(): ActorRef = {
    val props = KafkaPointsProducer.props(
      prefix = "kafka",
      config = config.popeyeProducerConfig,
      idGenerator = context.idGenerator,
      kafkaClient = new KafkaPointsClientFactory(config.producerConfig),
      metricRegistry = context.metrics,
      akkaDispatcher = Some(config.producerDispatcher)
    ).withRouter(FromConfig())
    context.actorSystem.actorOf(props, "kafka-producer")
  }

  def startReader(group: String, mainSink: PointsSink, dropSink: PointsSink): Unit = {
    val queueSizeGauge = new KafkaQueueSizeGauge(config.zkConnect, config.brokersList, group, config.topic)
    queueSizeGauge.start(config.queueSizePollInterval, actorSystem.scheduler)(context.ectx)
    readerMetrics.registerQueueSizeGauge(queueSizeGauge, group)
    val nWorkers = config.consumerWorkers
    val topic = config.topic
    for (i <- 0 until nWorkers) {
      consumerId += 1
      val consumerName = s"kafka-consumer-$group-$topic-$consumerId"
      val consumerConfig: KafkaPointsConsumerConfig = config.pointsConsumerConfig
      val pointsSource = getPointsSource(group, consumerName)
      val props = KafkaPointsConsumer.props(
        consumerName,
        consumerConfig,
        context.metrics,
        pointsSource,
        mainSink,
        dropSink,
        packedPoints => SendAndDrop(pointsToSend = packedPoints),
        context.idGenerator,
        context.ectx
      ).withDispatcher(config.consumerDispatcher)

      actorSystem.actorOf(props, consumerName)
    }
  }

  def getPointsSource(groupId: String, consumerId: String): PointsSource = {
    val consumerConfig: ConsumerConfig = config.createConsumerConfig(groupId)
    val consumerConnector = Consumer.create(consumerConfig)
    val sourceMetrics = new KafkaPointsSourceImplMetrics(f"$consumerId.source", metrics)
    new KafkaPointsSourceImpl(consumerConnector, config.topic, sourceMetrics)
  }

}

object KafkaPipelineChannel {

  def apply(config: Config, context: PipelineContext): KafkaPipelineChannel = {
    val c = KafkaPipelineChannelConfig(config)
    new KafkaPipelineChannel(c, context)
  }

  def factory(): PipelineChannelFactory = {
    new PipelineChannelFactory {
      override def make(config: Config, context: PipelineContext): PipelineChannel = {
        KafkaPipelineChannel(config, context)
      }
    }
  }
}

class KafkaReaderMetrics(val metricRegistry: MetricRegistry) extends Instrumented {
  def registerQueueSizeGauge(kafkaQueueSizeGauge: KafkaQueueSizeGauge, groupId: String): Unit = {
    val prefix = f"kafka.$groupId.queue.size"
    metrics.gauge(f"$prefix.total") {
      kafkaQueueSizeGauge.getTotalQueueSizeOption.getOrElse(-1l)
    }
    metrics.gauge(f"$prefix.max") {
      kafkaQueueSizeGauge.getMaxQueueSizeOption.getOrElse(-1l)
    }
  }
}
