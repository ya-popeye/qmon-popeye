package popeye.monitoring

import scala.util.Try
import kafka.api.{TopicMetadataRequest, TopicMetadataResponse}
import kafka.consumer.SimpleConsumer


class KafkaMonitoring(brokers: Set[(String, Int)], topics: Set[String]) {

  def sendMetadataRequests(broker: (String, Int), topics: Set[String]): Try[TopicMetadataResponse] = Try {
    val (host, port) = broker
    val clientId = "monitoring"
    val consumer = new SimpleConsumer(
      host,
      port,
      soTimeout = 2000,
      bufferSize = 2000,
      clientId)
    consumer.send(TopicMetadataRequest(versionId = 0, correlationId = 0, clientId, topics.toSeq))
  }

  def getMinISR(response: TopicMetadataResponse): Int = {
    val isrSizes = for {
      topicMetadata <- response.topicsMetadata
      partitionMetadata <- topicMetadata.partitionsMetadata
    } yield partitionMetadata.isr.size
    isrSizes.min
  }

  def getStats: Map[String, String] = {
    val brokerAndMetadataResponses = brokers.map {
      broker =>
        (broker, sendMetadataRequests(broker, topics))
    }
    val failedBrokers = brokerAndMetadataResponses.filter {case (_, responseTry) => responseTry.isFailure}.map(_._1)
    val minISR = brokerAndMetadataResponses.map(_._2).find(_.isSuccess).map(rTry => getMinISR(rTry.get)).getOrElse(-1)
    Map(
      "failed_brokers" -> failedBrokers.map(_._1).mkString("[", ", ", "]")
    )
  }

}
