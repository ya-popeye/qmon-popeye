package popeye.storage.opentsdb

import akka.actor.{Props, Actor, ActorLogging}
import org.hbase.async.HBaseClient
import net.opentsdb.core.TSDB
import com.typesafe.config.Config
import popeye.transport.proto.Message.Batch
import scala.collection.mutable.ArrayBuffer
import popeye.uuid.IdGenerator
import popeye.transport.proto.Storage.Ensemble
import popeye.transport.kafka.{ConsumeFailed, ConsumeDone, ConsumePending}

/**
 * @author Andrey Stepachev
 */
object TsdbWriter {

  def HBaseClient(tsdbConfig: Config) = {
    val zkquorum = tsdbConfig.getString("zk.cluster")
    val zkpath = tsdbConfig.getString("zk,path")
    new HBaseClient(zkquorum, zkpath);
  }

  def TSDB(hbaseClient: HBaseClient, tsdbConfig: Config) = {
    val seriesTable = tsdbConfig.getString("table.series")
    val uidsTable = tsdbConfig.getString("table.uids")
    new TSDB(hbaseClient, seriesTable, uidsTable);
  }
}


class TsdbWriterActor(tsdb: TSDB) extends Actor with ActorLogging {

  log.info("Started TSDB Writer")

  def receive = {
    case msg@ConsumePending(data, id) =>
      val client = sender
      log.debug("Processing {}", msg)
      new EventPersistFuture(tsdb, data) {
        protected def complete() {
          client ! ConsumeDone(id)
          if (log.isDebugEnabled)
            log.debug("Processing of {} complete: {}", msg)
        }

        protected def fail(cause: Throwable) {
          client ! ConsumeFailed(id, cause)
          if (log.isDebugEnabled)
            log.debug("Processing of {} failed: {}", msg, cause)
        }
      }
  }
}
