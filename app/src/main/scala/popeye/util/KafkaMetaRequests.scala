package popeye.util

import kafka.consumer.SimpleConsumer
import scala.util.{Failure, Success, Try}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.{ErrorMapping, TopicAndPartition}

trait IKafkaMetaRequests {
  def fetchEarliestOffsets(): Map[Int, Long] = {
    fetchOffsets(OffsetRequest.EarliestTime)
  }

  def fetchLatestOffsets(): Map[Int, Long] = {
    fetchOffsets(OffsetRequest.LatestTime)
  }

  def fetchOffsets(time: Long): Map[Int, Long]
}

object KafkaMetaRequests {
  val nonCriticalBrokerErrors = Set(
    ErrorMapping.LeaderNotAvailableCode,
    ErrorMapping.UnknownTopicOrPartitionCode,
    ErrorMapping.NotLeaderForPartitionCode
  )

}

class KafkaMetaRequests(brokers: Seq[(String, Int)],
                        topic: String,
                        clientId: String = "",
                        consumerSoTimeout: Int = 5000,
                        consumerBufferSize: Int = 10000) extends IKafkaMetaRequests {
  require(brokers.nonEmpty, "brokers seq is empty")

  class KafkaMetaRequestsException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)

  def fetchOffsets(time: Long): Map[Int, Long] = {
    val partitionsRequestAttempts = tryAllBrokers(requestPartitionIdsMapping(topic))
    val partitionsMapping: Map[(String, Int), Seq[Int]] = partitionsRequestAttempts.collect {case Success(ids) => ids}.headOption
      .getOrElse(partitionsRequestAttempts.head.get)
    val partitions = partitionsMapping.values.toSeq.flatten
    val offsetsRequestAttempts = tryAllBrokers(requestOffsets(topic, partitions, time))
    val offsetsMap = offsetsRequestAttempts
      .collect {
      case Success(offsets) => offsets
      case Failure(e: KafkaMetaRequestsException) => throw e
    }
      .foldLeft(Map.empty[Int, Long])(_ ++ _)
    offsetsMap
  }

  private def tryAllBrokers[T](operation: SimpleConsumer => T) = brokers.view.map {
    case (host, port) =>
      val consumer = new SimpleConsumer(host, port, consumerSoTimeout, consumerBufferSize, clientId)
      Try {
        try {
          operation(consumer)
        } finally {
          consumer.close()
        }
      }
  }

  private def requestOffsets(topic: String, partitionIds: Seq[Int], time: Long)
                            (consumer: SimpleConsumer): Map[Int, Long] = {
    val topicAndPartitions = partitionIds.map(i => TopicAndPartition(topic, i))
    val offsetReqInfo = PartitionOffsetRequestInfo(time, maxNumOffsets = 1)
    val offsetsRequest = OffsetRequest(requestInfo = topicAndPartitions.map(tap => tap -> offsetReqInfo).toMap)
    val offsets = consumer.getOffsetsBefore(offsetsRequest)
    val partitionAndOffsets =
      for {
        (tap, offsetResponse) <- offsets.partitionErrorAndOffsets
        if ! KafkaMetaRequests.nonCriticalBrokerErrors.contains(offsetResponse.error)
      }
      yield {
        if (offsetResponse.error != ErrorMapping.NoError) {
          val errorMsg = f"offsets request $offsetsRequest failed on broker ${consumer.host}:${consumer.port}"
          throw new KafkaMetaRequestsException(errorMsg, ErrorMapping.exceptionFor(offsetResponse.error))
        }
        (tap.partition, offsetResponse.offsets.head)
      }
    partitionAndOffsets.toMap
  }

  private def requestPartitionIdsMapping(topic: String)(consumer: SimpleConsumer): Map[(String, Int), Seq[Int]] = {
    val request = new TopicMetadataRequest(Seq(topic), 0)
    val metadata = consumer.send(request).topicsMetadata
    require(metadata.size == 1)
    metadata.head.partitionsMetadata.groupBy(_.leader).toList.collect {
      case (Some(leader), partitionMetas) =>
        val hostAndPort = (leader.host, leader.port)
        (hostAndPort, partitionMetas.map(_.partitionId))
    }.toMap
  }
}
