package popeye.storage.opentsdb

import akka.actor.{FSM, ActorRef, Actor, ActorLogging}
import org.hbase.async.HBaseClient
import net.opentsdb.core.TSDB
import com.typesafe.config.Config
import scala.concurrent.duration._
import popeye.storage.opentsdb.TsdbWriter._
import popeye.transport.proto.Message.{Event => PEvent}
import scala.collection.immutable
import scala.collection.JavaConversions.iterableAsScalaIterable
import popeye.transport.kafka.ConsumeDone
import popeye.transport.kafka.ConsumeFailed
import popeye.transport.kafka.ConsumePending
import popeye.transport.kafka.ConsumeId

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

  sealed trait State

  case object Idle extends State

  case object Active extends State

  sealed case class Todo(eventsCnt: Long = 0, queue: immutable.Seq[EventsPack] = Vector.empty)

  sealed case class Flush()

}


class TsdbWriter(tsdb: TSDB) extends Actor with FSM[State, Todo] with ActorLogging {

  startWith(Idle, Todo())

  when(Idle) {
    case Event(Flush,_) =>
      stay // nothing to do
  }

  when(Active, stateTimeout = 50 milliseconds) {
    case Event(Flush | StateTimeout, t: Todo) ⇒
      goto(Idle) using Todo()
  }

  onTransition {
    case Active -> Idle ⇒
      stateData match {
        case t @ Todo(eventsCnt, queue) ⇒
          if (!queue.isEmpty) {
            flush(t)
          }
      }
  }

  private def flush(t: Todo) {
    if (log.isDebugEnabled)
      log.debug("Flushing queue {} ({} events)", t.queue.size, t.eventsCnt)
    val sent = t.queue.map(pack => (pack.id, pack.sender))
    val data: Array[PEvent] = t.queue.flatMap(_.data).toArray
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
        log.error(cause, "Processing of batches {} failed", sent map {_._1})
      }
    }

  }

  whenUnhandled {
    case Event(ConsumePending(data, id), t: Todo) =>
      val list = data.getEventsList
      val pack = new EventsPack(list, sender, id)
      if (log.isDebugEnabled)
        log.debug("Queued {} (packs {}, events {} queued)", id, t.queue.size, t.eventsCnt)
      if (t.eventsCnt > 25000)
        self ! Flush()
      goto(Active) using t.copy(eventsCnt = t.eventsCnt + list.size(), queue = t.queue :+ pack)
    case Event(e, s) ⇒
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  onTermination {
    case StopEvent(cause, state, todo) =>
       flush(todo)
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
