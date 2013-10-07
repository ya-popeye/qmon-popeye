package popeye.util.hbase

import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.HTablePool
import java.io.Closeable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import scala.collection.JavaConversions._


/**
 * @author Andrey Stepachev
 */
class HBaseConfigured(config: Config, zkQuorum: String, poolSize: Int) extends Closeable {
  val hbaseConfiguration = makeHBaseConfig(config)
  hbaseConfiguration.set("hbase.zookeeper.quorum", zkQuorum)
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

}
