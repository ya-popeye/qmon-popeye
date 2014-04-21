package popeye.kafka

import kafka.consumer.SimpleConsumer
import kafka.api.TopicMetadataRequest
import scala.util.{Failure, Try}

class KafkaSimpleConsumerFactory(brokersString: String, consumerTimeout: Int, consumerBufferSize: Int, clientId: String) {
  val brokers: Seq[(String, Int)] = brokersString.split(",").map {
    str =>
      val tokens = str.split(":")
      (tokens(0), tokens(1).toInt)
  }

  def startConsumer(topic: String, partition: Int): SimpleConsumer = {
    val (host, port) = findLeader(topic, partition)
    new SimpleConsumer(host, port, consumerTimeout, consumerBufferSize, clientId)
  }

  private def findLeader(topic: String, partition: Int): (String, Int) = {
    val lazyResults = for (broker <- brokers.view) yield {
      val (host, port) = broker
      val consumer = new SimpleConsumer(host, port, consumerTimeout, consumerBufferSize, clientId)
      tryGetLeader(consumer, topic, partition)
    }
    val successfulTry = lazyResults.find(_.isSuccess).getOrElse {
      val factoryExceptionOption = lazyResults
        .collect {case Failure(e: KafkaSimpleConsumerFactoryException) => e}.headOption
      val exception = factoryExceptionOption.getOrElse {
        lazyResults.head.failed.get
      }
      throw exception
    }
    successfulTry.get
  }

  private def tryGetLeader(consumer: SimpleConsumer, topic: String, partition: Int): Try[(String, Int)] = Try {
    try {
      val metadataRequest = new TopicMetadataRequest(Seq(topic), correlationId = 0)
      val response = consumer.send(metadataRequest)
      val topicMetadata = response.topicsMetadata.find(_.topic == topic)
        .getOrElse(throw new KafkaSimpleConsumerFactoryException(f"no such topic $topic"))

      val partitionMetadata = topicMetadata.partitionsMetadata.find(_.partitionId == partition)
        .getOrElse(throw new KafkaSimpleConsumerFactoryException(f"no such partition $topic-$partition"))

      val leader = partitionMetadata.leader
        .getOrElse(throw new KafkaSimpleConsumerFactoryException(f"no leader for $topic-$partition"))
      (leader.host, leader.port)
    } finally {
      consumer.close()
    }
  }

}

class KafkaSimpleConsumerFactoryException(msg: String) extends RuntimeException(msg)

