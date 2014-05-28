package popeye.pipeline.kafka

import popeye.pipeline.{PipelineSinkFactory, PointsSink}
import com.typesafe.config.Config
import akka.actor.{ActorRef, ActorSystem}
import popeye.IdGenerator
import com.codahale.metrics.MetricRegistry
import scala.concurrent.{Promise, Future, ExecutionContext}
import kafka.producer.ProducerConfig
import popeye.proto.PackedPoints
import popeye.pipeline.config.KafkaPointsSinkConfigParser

case class KafkaPointsSinkConfig(producerConfig: ProducerConfig, pointsProducerConfig: KafkaPointsProducerConfig)

class KafkaSinkFactory(sinkFactory: KafkaSinkStarter)
  extends PipelineSinkFactory {
  def startSink(sinkName: String, config: Config): PointsSink = {
    val sinkConfig = KafkaPointsSinkConfigParser.parse(config.getConfig("kafka"))
    sinkFactory.startSink(sinkName, sinkConfig)
  }
}

class KafkaSinkStarter(actorSystem: ActorSystem,
                       ectx: ExecutionContext,
                       idGenerator: IdGenerator,
                       metrics: MetricRegistry) {
  def startSink(name: String,
                config: KafkaPointsSinkConfig) = {
    val kafkaClient = new PopeyeKafkaProducerFactoryImpl(config.producerConfig)
    val props = KafkaPointsProducer.props(f"$name.kafka", config.pointsProducerConfig, idGenerator, kafkaClient, metrics)
    val producerActor = actorSystem.actorOf(props, f"$name-kafka-producer")
    new KafkaPointsSink(producerActor)(ectx)
  }
}

class KafkaPointsSink(producer: ActorRef)(implicit eCtx: ExecutionContext) extends PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
    val promise = Promise[Long]()
    KafkaPointsProducer.produce(producer, Some(promise), points)
    val pointsInPack = points.pointsCount
    promise.future map {batchId => pointsInPack.toLong}
  }
}
