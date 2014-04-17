package popeye.test

import java.nio.file.{FileSystems, Files}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import java.net.InetSocketAddress
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer

class EmbeddedZookeeper(maxConnections: Int = 100) {
  val tmpPath = FileSystems.getDefault.getPath(System.getProperty("java.io.tmpdir"))
  val snapshotDir = Files.createTempDirectory(tmpPath, "popeye-zookeeper-test-sn").toFile
  val logDir = Files.createTempDirectory(tmpPath, "popeye-zookeeper-test-log").toFile
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 3000)
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress("localhost", /*any port*/ 0), maxConnections)
  factory.startup(zookeeper)


  def connectString = f"localhost:${factory.getLocalPort}"

  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  def client = new ZkClient(connectString, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)


  def shutdown() {
    factory.shutdown()
    snapshotDir.delete()
    logDir.delete()
  }
}
