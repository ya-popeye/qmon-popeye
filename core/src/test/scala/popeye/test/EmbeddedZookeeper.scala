package popeye.test

import java.nio.file.{FileSystems, Files}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import java.net.InetSocketAddress
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import popeye.pipeline.AtomicList

class EmbeddedZookeeper(maxConnections: Int = 100) {
  val tmpPath = FileSystems.getDefault.getPath(System.getProperty("java.io.tmpdir"))
  val snapshotDir = Files.createTempDirectory(tmpPath, "popeye-zookeeper-test-sn").toFile
  val logDir = Files.createTempDirectory(tmpPath, "popeye-zookeeper-test-log").toFile
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 3000)
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress("127.0.0.1", /*any port*/ 0), maxConnections)
  factory.startup(zookeeper)

  val clients = new AtomicList[ZkClient]

  def connectString = f"127.0.0.1:${factory.getLocalPort}"

  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  def newClient = {
    val client = new ZkClient(connectString, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    clients.add(client)
    client
  }


  def shutdown() {
    clients.foreach(_.close())
    factory.shutdown()
    snapshotDir.delete()
    logDir.delete()
  }
}
