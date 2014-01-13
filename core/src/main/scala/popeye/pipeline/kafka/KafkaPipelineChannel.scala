package popeye.pipeline.kafka

import akka.actor.{ActorRef, ActorSystem}
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.IdGenerator
import popeye.pipeline._
import popeye.proto.PackedPoints
import scala.concurrent.{ExecutionContext, Promise}
import akka.routing.FromConfig

/**
 * @author Andrey Stepachev
 */
class KafkaPipelineChannel(val config: Config,
                           val actorSystem: ActorSystem,
                           executionContext: ExecutionContext,
                           val metrics: MetricRegistry,
                           val idGenerator: IdGenerator)
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
    val producerConfig = KafkaPointsProducer.producerConfig(config)
    val kafkaClient = new PopeyeKafkaProducerFactoryImpl(producerConfig)
    actorSystem.actorOf(KafkaPointsProducer.props("kafka", config, idGenerator, kafkaClient, metrics)
      .withRouter(FromConfig())
      .withDispatcher(config.getString("producer.dispatcher")), "kafka-producer")
  }

  def startReader(group: String, mainSink: PointsSink, dropSink: PointsSink): Unit = {
    consumerId += 1
    val props = KafkaPointsConsumer.props(
      config.getString("topic"),
      group,
      config,
      metrics,
      mainSink,
      dropSink,
      executionContext
    ).withDispatcher(config.getString("consumer.dispatcher"))

    actorSystem.actorOf(props, s"kafka-consumer-$group-$consumerId")
  }
}
