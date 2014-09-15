package popeye.pipeline.kafka

import com.typesafe.config.Config
import kafka.producer.ProducerConfig
import kafka.consumer.ConsumerConfig

import scala.concurrent.duration._

/**
 * @author Andrey Stepachev
 */
abstract class KafkaPipelineChannelConfig(val producerDispatcher: String,
                                          val consumerDispatcher: String,
                                          val producerConfig: ProducerConfig,
                                          val pointsConsumerConfig: KafkaPointsConsumerConfig,
                                          val popeyeProducerConfig: KafkaPointsProducerConfig,
                                          val topic: String,
                                          val brokersList: Seq[(String, Int)],
                                          val zkConnect: String,
                                          val consumerWorkers: Int,
                                          val queueSizePollInterval: FiniteDuration) {
  def createConsumerConfig(kafkaGroup: String): ConsumerConfig

  def brokersListString = brokersList.map { case (host, port) => f"$host:$port" }.mkString(",")
}

object KafkaPipelineChannelConfig {
  def apply(config: Config): KafkaPipelineChannelConfig = {
    val topic: String = config.getString("topic")
    val producerConfig = config.getConfig("producer").withFallback(config)
    val consumerConfig = config.getConfig("consumer").withFallback(config)
    val pointsConsumerConfig = KafkaPointsConsumerConfig(consumerConfig)
    val brokersList = producerConfig.getString("broker.list").split(",").map {
      brokerStr =>
        val Array(host, port) = brokerStr.split(":")
        (host, port.toInt)
    }
    val zkConnect = consumerConfig.getString("zk.quorum")
    val queueSizePollInterval = FiniteDuration(config.getMilliseconds("queue.size.poll.interval"), MILLISECONDS)
    new KafkaPipelineChannelConfig(
      producerDispatcher = config.getString("producer.dispatcher"),
      consumerDispatcher = config.getString("consumer.dispatcher"),
      producerConfig = KafkaPointsProducer.producerConfig(producerConfig),
      pointsConsumerConfig = pointsConsumerConfig,
      popeyeProducerConfig = new KafkaPointsProducerConfig(config),
      topic = topic,
      brokersList = brokersList,
      zkConnect = zkConnect,
      consumerWorkers = config.getInt("consumer.workers"),
      queueSizePollInterval
    ) {
      override def createConsumerConfig(kafkaGroup: String) = KafkaConsumer.consumerConfig(
        group = kafkaGroup,
        consumerTimeout = pointsConsumerConfig.tick,
        kafkaConfig = consumerConfig,
        brokerList = brokersListString,
        zkConnect = zkConnect
      )
    }
  }
}
