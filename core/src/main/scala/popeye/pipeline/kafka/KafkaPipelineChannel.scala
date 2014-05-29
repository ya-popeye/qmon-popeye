package popeye.pipeline.kafka

import akka.actor.ActorRef
import com.typesafe.config.Config
import popeye.pipeline._
import popeye.proto.PackedPoints
import scala.concurrent.Promise
import akka.routing.FromConfig


/**
 * @author Andrey Stepachev
 */
class KafkaPipelineChannel(val config: KafkaPipelineChannelConfig,
                           val context: PipelineContext)
  extends PipelineChannel {

  var producer: Option[ActorRef] = None
  var consumerId: Int = 0

  def newWriter(): PipelineChannelWriter = {
    if (producer.isEmpty)
      producer = Some(startProducer())
    new PipelineChannelWriter {
      def write(promise: Option[Promise[Long]], points: PackedPoints): Unit = {
        KafkaPointsProducer.produce(producer.get, promise, points)
      }
    }
  }

  private def startProducer(): ActorRef = {
    val props = KafkaPointsProducer.props(
      prefix = "kafka",
      config = config.popeyeProducerConfig,
      idGenerator = context.idGenerator,
      kafkaClient = new KafkaPointsSinkFactory(config.producerConfig),
      metricRegistry = context.metrics,
      akkaDispatcher = Some(config.producerDispatcher)
    ).withRouter(FromConfig())
    context.actorSystem.actorOf(props, "kafka-producer")
  }

  def startReader(group: String, mainSink: PointsSink, dropSink: PointsSink): Unit = {
    val nWorkers = config.consumerWorkers
    val topic = config.topic
    for (i <- 0 until nWorkers) {
      consumerId += 1
      val name = s"kafka-consumer-$group-$topic-$consumerId"
      val props = KafkaPointsConsumer.props(
        config.consumerConfigFactory(group),
        context.metrics,
        mainSink,
        dropSink,
        packedPoints => SendAndDrop(pointsToSend = packedPoints),
        context.idGenerator,
        context.ectx
      ).withDispatcher(config.consumerDispatcher)

      actorSystem.actorOf(props, name)
    }
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

