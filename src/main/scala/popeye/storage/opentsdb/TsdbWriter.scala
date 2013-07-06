package popeye.storage.opentsdb

import akka.actor.{Props, Actor, ActorLogging}
import org.hbase.async.HBaseClient
import net.opentsdb.core.TSDB
import com.typesafe.config.Config
import popeye.transport.proto.Message.Batch
import scala.collection.mutable.ArrayBuffer
import popeye.uuid.IdGenerator
import popeye.transport.proto.Storage.Ensemble

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


class TsdbWriterActor(tsdb: TSDB, workerId: Long = 0, datacenterId: Long = 0, sequence: Long = 0) extends Actor with ActorLogging {

  case object Message

  case class WriteBatch(batch: Ensemble)
  case class CompleteBatch(batchId: Long)

  var buf = new ArrayBuffer[EventSendFuture]

  def receive = {
    case CompleteBatch(batchId) =>
      buf = buf.filter(_.getBatchId != batchId)
    case WriteBatch(batch) =>
      val me = self
      val client = sender
      buf += new EventSendFuture(tsdb, batch) {
        protected def complete() {
          me ! CompleteBatch(batch.getBatchId)
          client ! CompleteBatch(batch.getBatchId)
        }

        protected def fail() {
          me ! CompleteBatch(batch.getBatchId)
        }
      }
  }
}
