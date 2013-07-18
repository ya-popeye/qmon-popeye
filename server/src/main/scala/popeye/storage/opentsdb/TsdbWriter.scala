package popeye.storage.opentsdb

import akka.actor.{ActorRef, Actor, ActorLogging}
import org.hbase.async.HBaseClient
import net.opentsdb.core.TSDB
import com.typesafe.config.Config
import scala.concurrent.duration._
import popeye.storage.opentsdb.TsdbWriter._
import popeye.transport.proto.Message.{Event => PEvent}
import scala.collection.JavaConversions.asScalaBuffer
import popeye.transport.kafka.{ConsumeDone, ConsumeFailed, ConsumePending, ConsumeId}
import popeye.BufferedFSM
import popeye.BufferedFSM.Todo

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

  class EventsPack(val data: java.util.List[PEvent], val sender: ActorRef, val id: ConsumeId)

}

class TsdbWriter(tsdb: TSDB) extends Actor with BufferedFSM[EventsPack] with ActorLogging {

  override val timeout: FiniteDuration = 50 milliseconds
  override val flushEntitiesCount: Int = 25000

  override def consumeCollected(todo: Todo[EventsPack]) = {
    val sent = todo.queue.map(pack => (pack.id, pack.sender))
    val data: Array[PEvent] = todo.queue.flatMap {
      pack => pack.data
    }.toArray
    new EventPersistFuture(tsdb, data) {
      protected def complete() {
        sent foreach {
          pair =>
            if (log.isDebugEnabled)
              log.debug("Processing of batch {} complete", pair._1)
            pair._2 ! ConsumeDone(pair._1)
        }
      }

      protected def fail(cause: Throwable) {
        sent foreach {
          pair =>
            pair._2 ! ConsumeFailed(pair._1, cause)
        }
        log.error(cause, "Processing of batches {} failed", sent map {
          _._1
        })
      }
    }
  }

  val handleMessage: TodoFunction = {
    case Event(ConsumePending(data, id), todo) =>
      val list = data.getEventsList
      val pack = new EventsPack(list, sender, id)
      if (log.isDebugEnabled)
        log.debug("Queued {} (packs {}, events {} queued)", id, todo.queue.size, todo.entityCnt)
      todo.copy(entityCnt = todo.entityCnt + list.size(), queue = todo.queue :+ pack)
  }

  initialize()

  override def preStart() {
    super.preStart()
    log.info("Started TSDB Writer")
  }

  override def postStop() {
    super.postStop()
    log.info("Stoped TSDB Writer")
  }
}
