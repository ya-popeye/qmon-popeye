package popeye.util

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

case class ZkClientConfiguration(zkConnect: ZkConnect, sessionTimeout: Int, connectionTimeout: Int) {
  def zkConnectString = zkConnect.toZkConnectString

  def createClient = new ZkClient(zkConnectString, sessionTimeout, connectionTimeout, ZKStringSerializer)
}
