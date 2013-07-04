package popeye.transport.test

import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import kafka.zk.EmbeddedZookeeper

/**
 * @author Andrey Stepachev
 */
object ZkTestUtils {
  val zkConnectString = "127.0.0.1:2182"
}

trait ZkTestSpec {

  val zkConnect: String = ZkTestUtils.zkConnectString
  var zookeeper: EmbeddedZookeeper = null
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  def withZk()(body: => Unit) {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    zkClient = new ZkClient(zookeeper.connectString, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    try {
      body
    } finally {
      zkClient.close()
      zookeeper.shutdown()
    }
  }
}
