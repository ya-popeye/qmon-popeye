package popeye.storage.opentsdb

import akka.actor._
import org.hbase.async.HBaseClient
import net.opentsdb.core.{EventPersistFuture, TSDB}
import com.typesafe.config.Config
import scala.concurrent.duration._
import popeye.storage.opentsdb.TsdbWriter._
import popeye.transport.proto.Message.{Event => PEvent}
import scala.collection.JavaConversions.asScalaBuffer
import popeye.transport.kafka.{ConsumeDone, ConsumeFailed, ConsumePending, ConsumeId}
import popeye.{Instrumented, BufferedFSM}
import popeye.BufferedFSM.Todo
import com.codahale.metrics.MetricRegistry
import akka.routing.FromConfig
import popeye.BufferedFSM.Todo
import popeye.transport.kafka.ConsumeDone
import popeye.transport.kafka.ConsumeFailed
import popeye.transport.kafka.ConsumePending
import popeye.transport.kafka.ConsumeId

/**
 * @author Andrey Stepachev
 */

case class TsdbWriterMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer("tsdb.write.write")
  val writeBatchSizeHist = metrics.histogram("tsdb.write.batch-size.write")
  val incomingBatchSizeHist = metrics.histogram("tsdb.write.batch-size.incoming")
}

class TsdbWriter(config: Config, tsdb: TSDB, metrics: TsdbWriterMetrics)
  extends Actor with BufferedFSM[EventsPack] with ActorLogging {

  override def timeout: FiniteDuration = new FiniteDuration(
    config.getMilliseconds("tsdb.flush.tick"), MILLISECONDS
  )
  override def flushEntitiesCount: Int = config.getInt("tsdb.flush.events")

  override def consumeCollected(todo: Todo[EventsPack]) = {
    val ctx = metrics.writeTimer.timerContext()
    val sent = todo.queue.map(pack => (pack.id, pack.sender))
    val data: Array[PEvent] = todo.queue.flatMap {
      pack => pack.data
    }.toArray
    metrics.writeBatchSizeHist.update(data.size)
    new EventPersistFuture(tsdb, data) {
      protected def complete() {
        val nanos = ctx.stop()
        sent foreach {
          pair =>
            pair._2 ! ConsumeDone(pair._1)
            if (log.isDebugEnabled)
              log.debug("Processing of batch {} complete in {}ns", pair._1, nanos)
        }
      }

      protected def fail(cause: Throwable) {
        val nanos = ctx.stop()
        sent foreach {
          pair =>
            pair._2 ! ConsumeFailed(pair._1, cause)
        }
        log.error(cause, "Processing of batches {} failed in {}ns", sent map {
          _._1
        }, nanos)
      }
    }
  }

  val handleMessage: TodoFunction = {
    case Event(ConsumePending(data, id), todo) =>
      val list = data.getEventsList
      val pack = new EventsPack(list, sender, id)
      metrics.incomingBatchSizeHist.update(data.getEventsCount)
      val t = todo.copy(entityCnt = todo.entityCnt + list.size(), queue = todo.queue :+ pack)
      if (log.isDebugEnabled)
        log.debug("Queued {} todo={} (list={})", id, t, list.size)
      t
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

object TsdbWriter {

  def start(config: Config, hbaseClient: Option[HBaseClient] = None)(implicit system: ActorSystem, metricSystem: MetricRegistry) : ActorRef = {
    val hbc = hbaseClient getOrElse new HBaseClient(config.getString("tsdb.zk.cluster"), config.getString("tsdb.zk.path"))
    val tsdb: TSDB = new TSDB(hbc,
      config.getString("tsdb.table.series"),
      config.getString("tsdb.table.uids"))
    system.registerOnTermination(tsdb.shutdown())
    system.registerOnTermination(hbc.shutdown())

    val metrics = TsdbWriterMetrics(metricSystem) // capture it
    system.actorOf(Props(new TsdbWriter(config, tsdb, metrics))
      .withRouter(FromConfig()), "tsdb-writer")
  }

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

