package popeye.transport.legacy

import akka.actor._
import akka.io._
import akka.util.ByteString
import akka.io.IO
import akka.io.TcpPipelineHandler.{WithinActorContext, Init}
import java.net.InetSocketAddress
import net.opentsdb.core.Tags
import popeye.transport.kafka.{ProduceDone, ProducePending}
import popeye.transport.proto.Message.{Tag, Event}
import scala.collection.mutable
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config

object TsdbTelnetHandlerProto {

  case object ReplyOk

  sealed case class ReplyErr(ex: Throwable)

}

class TsdbTelnetHandler(init: Init[WithinActorContext, String, String], connection: ActorRef, kafkaProducer: ActorRef, config: Config)
  extends Actor with ActorLogging {

  import TsdbTelnetHandlerProto._

  val pendingTimeout: akka.util.Timeout = 5 seconds
  // TODO: move to config
  val hwPendingPoints: Long = 2000
  val lwPendingPoints: Long = 1000
  require(hwPendingPoints > lwPendingPoints, "High watermark should be greater then low watermark")

  private val pendigCommits = new ListBuffer[(ActorRef, Long)]
  private var lastBatchId: Long = 0
  private var confirmedPointId: Long = 0
  private var pointId: Long = 0
  private var suspended = false

  final def receive = {
    case init.Event(data) =>
      if (data.length > 0) {
        val input = if (data.charAt(data.length - 1) == '\r') {
          data.dropRight(1)
        } else {
          data
        }
        try {
          val strings = Tags.splitString(input, ' ')
          strings(0) match {
            case "put" =>
              pointId += 1
              kafkaProducer ! ProducePending(pointId)(Seq(parsePoint(strings)))
              checkSuspension()
            case "commit" =>
              pendigCommits += (sender -> strings(1).toLong)
              self ! ReplyOk
            case "ver" =>
              self ! init.Command("OK unknown\n")
            case "exit" =>
              self ! init.Command("OK sayonara\n")
              self ! Tcp.ConfirmedClose
            case _ =>
              sender ! init.Command("OK Unknown command\n")
          }
        } catch {
          case ex: Exception =>
            sender ! init.Command("ERR " + ex.getMessage + "\n")
            context.stop(self)
        }
      }

    case ProduceDone(completePointId, batchId) =>
      if (lastBatchId < batchId)
        lastBatchId = batchId
      val last: Long = completePointId.sorted.last
      if (confirmedPointId < last)
        confirmedPointId = last
      checkSuspension()
      self ! ReplyOk

    case ReplyOk =>
      pendigCommits foreach {p =>
        p._1 ! init.Command("OK " + p._2 + "=" + lastBatchId + "\n")
      }
      pendigCommits.clear()

    case ReplyErr(ex) =>
      sender ! init.Command("ERR " + ex.getMessage + "\n")

    case x: Tcp.ConnectionClosed =>
      log.debug("connection closed")
      context.stop(self)
  }

  def checkSuspension() = {
    val pending = pointId - confirmedPointId
    if (pending > hwPendingPoints && !suspended) {
      self ! Tcp.SuspendReading
      suspended = true
      log.debug("Suspending client")

    }
    if (pending < lwPendingPoints && suspended) {
      self ! Tcp.ResumeReading
      suspended = false
      log.debug("Resuming client")
    }
  }

  def parsePoint(words: Array[String]): Event = {
    val ev = Event.newBuilder()
    words(0) = null; // Ditch the "put".
    if (words.length < 5) {
      // Need at least: metric timestamp value tag
      //               ^ 5 and not 4 because words[0] is "put".
      throw new IllegalArgumentException("not enough arguments"
        + " (need least 4, got " + (words.length - 1) + ')');
    }
    ev.setMetric(words(1));
    if (ev.getMetric.isEmpty) {
      throw new IllegalArgumentException("empty metric name");
    }
    ev.setTimestamp(Tags.parseLong(words(2)));
    if (ev.getTimestamp <= 0) {
      throw new IllegalArgumentException("invalid timestamp: " + ev.getTimestamp);
    }
    val value = words(3);
    if (value.length() <= 0) {
      throw new IllegalArgumentException("empty value");
    }
    if (Tags.looksLikeInteger(value)) {
      ev.setIntValue(Tags.parseLong(value));
    } else {
      // floating point value
      ev.setFloatValue(java.lang.Float.parseFloat(value));
    }
    parseTags(ev, 4, words)
    ev.build
  }

  /**
   * Parses tags into a Event.Tag structure.
   * @param tags String array of the form "tag=value".
   * @throws IllegalArgumentException if the tag is malformed.
   * @throws IllegalArgumentException if the tag was already in tags with a
   *                                  different value.
   */
  def parseTags(builder: Event.Builder, startIdx: Int, tags: Array[String]) {
    val set = mutable.HashSet[String]()
    for (i <- startIdx to tags.length - 1) {
      val tag = tags(i)
      val kv: Array[String] = Tags.splitString(tag, '=')
      if (kv.length != 2 || kv(0).length <= 0 || kv(1).length <= 0) {
        throw new IllegalArgumentException("invalid tag: " + tag)
      }
      if (!set.add(kv(0))) {
        throw new IllegalArgumentException("duplicate tag: " + tag + ", tags=" + tag)
      }
      builder.addTags(Tag.newBuilder()
        .setName(kv(0))
        .setValue(kv(1)))
    }
  }
}

class TsdbTelnetServer(local: InetSocketAddress, kafka: ActorRef)(implicit val metricRegistry: MetricRegistry) extends Actor with ActorLogging {

  import Tcp._

  implicit def system = context.system

  IO(Tcp) ! Bind(self, local)

  def receive: Receive = {
    case _: Bound ⇒
      context.become(bound(sender))
  }

  def bound(listener: ActorRef): Receive = {
    case Connected(remote, _) ⇒
      val init = TcpPipelineHandler.withLogger(log,
        new StringByteStringAdapter("utf-8") >>
          new DelimiterFraming(maxSize = 2048, delimiter = ByteString('\n'),
            includeDelimiter = false) >>
          new TcpReadWriteAdapter >>
          new BackpressureBuffer(lowBytes = 1 * 1024 * 1024, highBytes = 4 * 1024 * 1024, maxBytes = 10 * 1024 * 1024))

      val connection = sender
      val handler = context.actorOf(Props(new TsdbTelnetHandler(init, connection, kafka, system.settings.config))
        .withDeploy(Deploy.local))
      val pipeline = context.actorOf(TcpPipelineHandler.props(
        init, connection, handler).withDeploy(Deploy.local))

      connection ! Tcp.Register(pipeline)
  }

  override def preStart() {
    super.preStart()
    log.info("Started Tsdb Telnet server")
  }

  override def postStop() {
    super.postStop()
    log.info("Stoped Tsdb Telnet server")
  }
}

object TsdbTelnetServer {
  def start(config: Config, kafkaProducer: ActorRef)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    val hostport = config.getString("tsdb.telnet.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    system.actorOf(Props(new TsdbTelnetServer(addr, kafkaProducer)))
  }
}
