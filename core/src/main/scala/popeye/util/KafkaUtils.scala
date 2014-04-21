package popeye.util

import kafka.consumer.SimpleConsumer
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import kafka.admin.AdminUtils

object KafkaUtils {

  def fetchLatestOffsets(brokers: Seq[(String, Int)], topic: String, partitions: Seq[Int]): Map[Int, Long] = {
    def fetchOffsetsFrom(broker: (String, Int)) = {
      val (host, port) = broker
      val consumer = new SimpleConsumer(host, port, soTimeout = 1000, bufferSize = 10000, "drop")
      try {
        val topicAndPartitions = partitions.map(i => TopicAndPartition(topic, i))
        val offsetReqInfo = PartitionOffsetRequestInfo(OffsetRequest.LatestTime, maxNumOffsets = 10)
        val offsetsRequest = OffsetRequest(requestInfo = topicAndPartitions.map(tap => tap -> offsetReqInfo).toMap)
        val offsets = consumer.getOffsetsBefore(offsetsRequest)
        val partitionAndOffsets =
          for {
            (tap, offsetResponse) <- offsets.partitionErrorAndOffsets if offsetResponse.error == ErrorMapping.NoError
          }
          yield {
            (tap.partition, offsetResponse.offsets.head)
          }
        partitionAndOffsets.toMap
      } finally {
        consumer.close()
      }
    }
    brokers.map(broker => fetchOffsetsFrom(broker)).foldLeft(Map[Int, Long]())(_ ++ _)
  }

  def fetchPartitionsMetadata(kafkaZkConnect: String,
                              topic: String,
                              zkSessionTimeout: Int = 5000,
                              zkConnectionTimeout: Int = 5000) = {
    val zkClient = new ZkClient(kafkaZkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    try {
      val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)
      topicMetadata.partitionsMetadata
    } finally {
      zkClient.close()
    }
  }
}
