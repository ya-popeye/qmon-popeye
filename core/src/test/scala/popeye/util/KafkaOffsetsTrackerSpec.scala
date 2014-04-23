package popeye.util

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import popeye.test.{MockitoStubs, EmbeddedZookeeper}
import org.I0Itec.zkclient.ZkClient

class KafkaOffsetsTrackerSpec extends FlatSpec with Matchers with BeforeAndAfter with MockitoStubs {
  var zookeeper: EmbeddedZookeeper = null

  val offsetsPath: String = "/offsets"

  before {
    zookeeper = new EmbeddedZookeeper()
  }

  after {
    zookeeper.shutdown()
  }

  behavior of "KafkaOffsetsTracker"

  it should "initialize offsets file with empty string" in {
    val kafkaMeta = mock[IKafkaMetaRequests]
    kafkaMeta.fetchLatestOffsets() answers ((_) => Map(0 -> 1l))
    kafkaMeta.fetchEarliestOffsets() answers ((_) => Map(0 -> 1l))
    val tracker = new KafkaOffsetsTracker(kafkaMeta, zookeeper.connectString, offsetsPath)
    tracker.fetchOffsetRanges()
    val data: String = zookeeper.newClient.readData(offsetsPath)
    data should equal("")
  }

  it should "save offsets" in {
    val kafkaMeta = mock[IKafkaMetaRequests]
    kafkaMeta.fetchLatestOffsets() answers ((_) => Map(0 -> 1l))
    kafkaMeta.fetchEarliestOffsets() answers ((_) => Map(0 -> 0l))
    val tracker = new KafkaOffsetsTracker(kafkaMeta, zookeeper.connectString, offsetsPath)
    val offsetRanges = tracker.fetchOffsetRanges()
    tracker.commitOffsets(offsetRanges)
    val data: String = zookeeper.newClient.readData(offsetsPath)
    data should equal("0:1")
  }

  it should "load old offsets" in {
    val zkClient: ZkClient = zookeeper.newClient
    zkClient.createPersistent(offsetsPath, "0:10,1:0")
    val kafkaMeta = mock[IKafkaMetaRequests]
    kafkaMeta.fetchLatestOffsets() answers ((_) => Map(0 -> 100l, 1 -> 10l))
    kafkaMeta.fetchEarliestOffsets() answers ((_) => Map(0 -> 10l, 1 -> 0l))
    val tracker = new KafkaOffsetsTracker(kafkaMeta, zookeeper.connectString, offsetsPath)
    val offsetRanges = tracker.fetchOffsetRanges()
    offsetRanges(0) should equal(OffsetRange(10, 100))
    offsetRanges(1) should equal(OffsetRange(0, 10))
  }

  it should "detect new partitions" in {
    val zkClient: ZkClient = zookeeper.newClient
    zkClient.createPersistent(offsetsPath, "0:10,1:0")
    val kafkaMeta = mock[IKafkaMetaRequests]
    kafkaMeta.fetchLatestOffsets() answers ((_) => Map(0 -> 100l, 1 -> 10l, 2 -> 10))
    kafkaMeta.fetchEarliestOffsets() answers ((_) => Map(0 -> 10l, 1 -> 0l, 2 -> 1))
    val tracker = new KafkaOffsetsTracker(kafkaMeta, zookeeper.connectString, offsetsPath)
    val offsetRanges = tracker.fetchOffsetRanges()
    offsetRanges(2) should equal(OffsetRange(1, 10))
  }

}
