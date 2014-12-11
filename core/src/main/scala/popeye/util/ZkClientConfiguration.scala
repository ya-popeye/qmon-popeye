package popeye.util

case class ZkClientConfiguration(zkConnect: ZkConnect, sessionTimeout: Int, connectionTimeout: Int) {
  def zkConnectString = zkConnect.toZkConnectString
}
