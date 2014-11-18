package popeye.pipeline

import java.io.File
import java.util.Properties

import _root_.kafka.consumer.{ConsumerConfig, Consumer}
import _root_.kafka.producer.ProducerConfig
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{Config, ConfigFactory}
import popeye.{Logging, IdGenerator}
import popeye.pipeline.kafka._
import popeye.proto.Message

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Try}

object KafkaSmokeTest extends Logging {
  def main(args: Array[String]) {
    val conf = ConfigFactory.parseFile(new File(args(0)))
    val topic = conf.getString("topic")
    val consumerConf = conf.getConfig("consumer")
    val consumerConfig: ConsumerConfig = createConsumerConfig(consumerConf)
    val producerConf = conf.getConfig("producer")
    val producerConfig: ProducerConfig = createProducerConfig(producerConf)

    val metricRegistry = new MetricRegistry
    val consumerConnector = Consumer.create(consumerConfig)
    val sourceMetrics = new KafkaPointsSourceImplMetrics("points_source", metricRegistry)
    val pointsSource = new KafkaPointsSourceImpl(consumerConnector, topic, sourceMetrics)
    val producer = new PointsKafkaClient(topic, producerConfig)

    val consumerThread = new Thread(new Runnable {
      override def run(): Unit = {
        while(true) {
          val consumed = Try(pointsSource.consume())
          info("consumed:")
          info(consumed.toString)
        }
      }
    })
    consumerThread.setDaemon(true)
    consumerThread.start()

    val idGenerator = new IdGenerator(0)
    while(true) {
      val currentTime = (System.currentTimeMillis() / 1000).toInt
      val batchId = idGenerator.nextId()
      val attribute = Message.Attribute.newBuilder()
        .setName("cluster").setValue("popeye_test")
        .build()
      val point =
        Message.Point.newBuilder()
          .setMetric("test")
          .setValueType(Message.Point.ValueType.INT)
          .setIntValue(0)
          .setTimestamp(currentTime)
          .addAttributes(attribute)
          .build()

      val sendingAttempt = Try(Await.result(producer.sendPoints(batchId, point), Duration.Undefined))
      info(sendingAttempt.toString)
      if (sendingAttempt.isFailure) {
        val Failure(t) = sendingAttempt
        info(t)
      }
      System.in.read()
    }
  }

  def createProducerConfig(producerConf: Config): ProducerConfig = {
    val producerProps = new Properties()
    producerProps.setProperty("metadata.broker.list", producerConf.getString("metadata.broker.list"))
    producerProps.setProperty("key.serializer.class", classOf[KeySerialiser].getName)
    producerProps.setProperty("partitioner.class", classOf[KeyPartitioner].getName)
    val producerConfig = new ProducerConfig(producerProps)
    producerConfig
  }

  def createConsumerConfig(consumerConf: Config): ConsumerConfig = {
    val consumerProps = new Properties()
    consumerProps.setProperty("zookeeper.connect", consumerConf.getString("zookeeper.connect"))
    consumerProps.setProperty("metadata.broker.list", consumerConf.getString("metadata.broker.list"))
    consumerProps.setProperty("group.id", consumerConf.getString("group.id"))
    consumerProps.setProperty("consumer.timeout.ms", consumerConf.getString("consumer.timeout.ms"))
    val consumerConfig = new ConsumerConfig(consumerProps)
    consumerConfig
  }
}
