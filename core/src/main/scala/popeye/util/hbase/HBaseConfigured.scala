package popeye.util.hbase

import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.HTablePool
import java.io.Closeable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import scala.collection.JavaConversions._
import org.apache.zookeeper.ZooKeeper
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuffer
import spray.http.Uri.IPv6Host
import popeye.Logging


/**
 * @author Andrey Stepachev
 */
class HBaseConfigured(config: Config, zkQuorum: String, poolSize: Int) extends Closeable with Logging {
  val hbaseConfiguration = makeHBaseConfig(config)
  log.info("using quorum: {}", zkQuorum)
  private val parts: Array[String] = zkQuorum.split("/", 2)
  hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, parts(0))
  updateZkPort(parts(0))
  if (parts.length > 1) {
    val zkChroot: String = "/" + parts(1)
    hbaseConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkChroot)
    log.info("hbase chrooted to: {}", zkChroot)
  }
  val hTablePool = new HTablePool(hbaseConfiguration, poolSize)


  def close(): Unit = {
    hTablePool.close()
  }

  private def makeHBaseConfig(config: Config): Configuration = {
    val hbaseConfig = HBaseConfiguration.create
    config.entrySet() foreach { entry =>
      hbaseConfig.set(entry.getKey, entry.getValue.unwrapped().toString())
    }
    hbaseConfig
  }

  def updateZkPort(zkQuorum: String) = {

    val tokenizer: StringTokenizer = new StringTokenizer(zkQuorum, ",")
    val ports = new ArrayBuffer[String]()
    while (tokenizer.hasMoreTokens) {
      val hostToken: String = tokenizer.nextToken
      val hostPort = hostToken.split(':')
      if (hostPort(1).trim.length > 0)
        ports.add(hostPort(1).trim)
    }
    val uniq = ports.toSet
    uniq.size match {
      case 0 =>
        // nothing to do, using default port
      case 1 =>
        // ok, we got nondefault port
        hbaseConfiguration.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, uniq.head.toInt)
        log.info("hbase zk port: {}", uniq.head.toInt)
      case _ =>
        throw new IllegalStateException("Found more then one zk port, " +
          "hbase doesn't understand different zk ports for different servers: " + uniq.toString())
    }
  }
}
