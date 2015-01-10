package popeye.util

import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer

case class OffsetRange(startOffset: Long, stopOffset: Long) {
  require(startOffset <= stopOffset, "startOffset > stopOffset")
}

class KafkaOffsetsTracker(kafkaMetaRequests: IKafkaMetaRequests,
                          zkClientConfig: ZkClientConfiguration,
                          offsetsPath: String) {

  def fetchOffsetRanges(): Map[Int, OffsetRange] = {
    val latestOffsets = kafkaMetaRequests.fetchLatestOffsets()
    val earliestOffsets = kafkaMetaRequests.fetchEarliestOffsets()
    val consumedOffsets = withZkClient(loadOffsets)
    latestOffsets.toList.map {
      case (partitionId, latestOffset) =>
        partitionId -> OffsetRange(
          startOffset = consumedOffsets.getOrElse(partitionId, earliestOffsets(partitionId)),
          stopOffset = latestOffset
        )
    }.toMap
  }

  def commitOffsets(offsetRanges: Map[Int, OffsetRange]) = withZkClient {
    zkClient =>
      val oldOffsets = loadOffsets(zkClient)
      checkModifications(offsetRanges, oldOffsets)
      val newOffsets = offsetRanges.mapValues(_.stopOffset)
      zkClient.writeData(offsetsPath, serializeOffsets(newOffsets))
      val savedOffsets = loadOffsets(zkClient)
      if (savedOffsets != newOffsets) {
        throw new RuntimeException("Race")
      }
  }

  private def checkModifications(offsetRanges: Map[Int, OffsetRange], oldOffsets: Map[Int, Long]) = {
    for ((partitionId, oldOffset) <- oldOffsets) {
      val offsetRange =
        offsetRanges.getOrElse(partitionId, throw new RuntimeException(f"stale offsets ranges: unknown partition $partitionId"))
      if (offsetRange.startOffset != oldOffset) {
        throw new RuntimeException(f"stale offsets ranges: old offset for partition $partitionId")
      }
    }
    for ((partitionId, offsetRange) <- offsetRanges) {
      val oldOffset = oldOffsets.getOrElse(partitionId, 0l)
      if (offsetRange.startOffset != oldOffset) {
        throw new RuntimeException(f"stale offsets ranges: old offset for partition $partitionId")
      }
    }
  }

  private def loadOffsets(zkClient: ZkClient): Map[Int, Long] = {
    if (!zkClient.exists(offsetsPath)) {
      val createParents = true
      zkClient.createPersistent(offsetsPath, createParents)
      zkClient.writeData(offsetsPath, "")
    }
    val offsetsString: String = zkClient.readData(offsetsPath)
    parseOffsets(offsetsString)
  }

  private def parseOffsets(offsetsString: String): Map[Int, Long] = {
    offsetsString.split(",").filter(_.nonEmpty).map {
      partitionAndOffset =>
        val tokens = partitionAndOffset.split(":")
        (tokens(0).toInt, tokens(1).toLong)
    }.toMap
  }

  private def serializeOffsets(offsets: Map[Int, Long]) = {
    offsets.toList.map {
      case (partition, offset) => f"$partition:$offset"
    }.mkString(",")
  }

  private def withZkClient[T](operation: ZkClient => T): T = {

    val zkClient = zkClientConfig.createClient
    try {
      operation(zkClient)
    } finally {
      zkClient.close()
    }
  }
}
