package popeye.util.hbase

import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.HTablePool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import popeye.util.ZkConnect
import scala.collection.JavaConversions._
import popeye.Logging


/**
 * @author Andrey Stepachev
 */
class HBaseConfigured(config: Config, zkConnect: ZkConnect) extends Logging {
  val hbaseConfiguration = {
    val conf = makeHBaseConfig(config)
    log.info("using quorum: {}", zkConnect)
    val (hosts, portOptions) = zkConnect.hostAndPorts.unzip
    val portsSet = portOptions.toSet
    require(
      portsSet.size == 1,
      s"Found more than one zk port, hbase doesn't understand different zk ports for different servers: $zkConnect"
    )
    conf.set(HConstants.ZOOKEEPER_QUORUM, hosts.mkString(","))
    val portOption = portsSet.head
    for (port <- portOption) {
      conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, port)
      log.info("hbase zk port: {}", port)
    }
    for (chrootStr <- zkConnect.chroot) {
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, chrootStr)
      log.info("hbase chrooted to: {}", chrootStr)
    }
    conf
  }

  def getHTablePool(size: Int) = new HTablePool(hbaseConfiguration, size)

  private def makeHBaseConfig(config: Config): Configuration = {
    val hbaseConfig = HBaseConfiguration.create
    config.entrySet() foreach { entry =>
      hbaseConfig.set(entry.getKey, entry.getValue.unwrapped().toString())
    }
    hbaseConfig
  }
}
