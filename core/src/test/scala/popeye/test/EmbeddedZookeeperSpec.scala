package popeye.test

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

class EmbeddedZookeeperSpec extends FlatSpec with Matchers with BeforeAndAfter {
  behavior of "EmbeddedZookeeper"

  var zookeeper: EmbeddedZookeeper = null

  before {
    zookeeper = new EmbeddedZookeeper()
  }

  after {
    zookeeper.shutdown()
  }

  it should "perform read write ops" in {
    val zkClient = zookeeper.client
    zkClient.createEphemeral("/test", "test data")
    val str: String = zkClient.readData("/test")
    str should equal("test data")
    zkClient.close()
  }
}
