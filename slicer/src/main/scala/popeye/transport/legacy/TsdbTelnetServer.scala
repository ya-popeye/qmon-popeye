package popeye.transport.legacy

import akka.actor._
import akka.io._
import akka.util.ByteString
import akka.io.IO
import akka.io.TcpPipelineHandler.{WithinActorContext, Init}
import java.net.InetSocketAddress
import net.opentsdb.core.Tags
import popeye.transport.kafka.{ProduceNeedThrottle, ProduceDone, ProducePending}
import popeye.transport.proto.Message.{Attribute, Point}
import scala.collection.mutable
import scala.concurrent.duration._
import com.codahale.metrics.{Timer, MetricRegistry}
import com.typesafe.config.Config
import popeye.Instrumented
import com.yammer.metrics.core.TimerContext

class TsdbTelnetMetrics (override val metricRegistry: MetricRegistry) extends Instrumented {
  val requestTimer = metrics.timer("request-time")
  val commitTimer = metrics.timer("commit-time")
  val pointsRcvMeter = metrics.meter("points-received")
  val pointsCommitMeter = metrics.meter("points-commited")
}

class TsdbTelnetHandler(init: Init[WithinActorContext, String, String], connection: ActorRef, kafkaProducer: ActorRef,
                        config: Config, metrics: TsdbTelnetMetrics)
  extends Actor with ActorLogging {


  // TODO: move to config
  val hwPendingPoints: Int = config.getInt("legacy.tsdb.high-watermark")
  val lwPendingPoints: Int = config.getInt("legacy.tsdb.low-watermark")
  require(hwPendingPoints > lwPendingPoints, "High watermark should be greater then low watermark")

  type PointId = Long
  type BatchId = Long
  type CorrelationId = Long

  sealed case class CommitReq(sender: ActorRef, pointId: PointId, correlation: CorrelationId, timerContext: Timer.Context)

  private var pendingCommits: Seq[CommitReq] = Vector()
  private val pendingPoints = mutable.TreeSet[PointId]()
  private var pendingExit = false
  private var lastBatchId: BatchId = 0
  private var pointId: PointId = 0
  private var suspended = false

  private var requestTimer = metrics.requestTimer.timerContext()

  override def postStop() {
    super.postStop()
    requestTimer.close()
  }

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
              pendingPoints.add(pointId)
              metrics.pointsRcvMeter.mark()
            case "commit" =>
              pendingCommits = (pendingCommits :+ CommitReq(sender, pointId, strings(1).toLong,
                metrics.commitTimer.timerContext())).sortBy(_.pointId)
            case "ver" =>
              sender ! init.Command("OK unknown\n")
            case "exit" =>
              pendingExit = true
            case c @ _ =>
              sender ! init.Command(s"ERR Unknown command $c\n")
              context.stop(self)
          }
        } catch {
          case ex: Exception =>
            sender ! init.Command("ERR " + ex.getMessage + "\n")
            context.stop(self)
        }
      }
      tryReplyOk()
      checkSuspension()

    case ProduceNeedThrottle =>
      connection ! Tcp.SuspendReading
      suspended = true

    case ProduceDone(completePointId, batchId) =>
      if (lastBatchId < batchId) {
        lastBatchId = batchId
      }
      pendingPoints --= completePointId
      val commitedSize: Long = completePointId.size
      metrics.pointsCommitMeter.mark(commitedSize)
      tryReplyOk()
      checkSuspension()

    case x: Tcp.ConnectionClosed =>
      context.stop(self)
  }

  def tryReplyOk() = {

    val minPoint: Long = pendingPoints.headOption getOrElse Long.MaxValue
    pendingCommits.span(_.pointId < minPoint) match {
      case (complete, incomplete) =>
        complete foreach {
          p =>
            p.sender ! init.Command("OK " + p.correlation + "=" + lastBatchId + "\n")
            p.timerContext.stop()
        }
        pendingCommits = incomplete
    }

    if (pendingExit && pendingCommits.isEmpty) {
      connection ! Tcp.ConfirmedClose
    }
  }

  def checkSuspension() = {
    val size: Int = pendingPoints.size
    if (size > hwPendingPoints && !suspended) {
      connection ! Tcp.SuspendReading
      suspended = true
    }

    if (size < lwPendingPoints && suspended) {
      connection ! Tcp.ResumeReading
      suspended = false
    }
  }

  def parsePoint(words: Array[String]): Point = {
    val ev = Point.newBuilder()
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
   * Parses tags into a Point.Attribute structure.
   * @param tags String array of the form "tag=value".
   * @throws IllegalArgumentException if the tag is malformed.
   * @throws IllegalArgumentException if the tag was already in tags with a
   *                                  different value.
   */
  def parseTags(builder: Point.Builder, startIdx: Int, tags: Array[String]) {
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
      builder.addAttributes(Attribute.newBuilder()
        .setName(kv(0))
        .setValue(kv(1)))
    }
  }
}

class TsdbTelnetServer(local: InetSocketAddress, kafka: ActorRef, metrics: TsdbTelnetMetrics) extends Actor with ActorLogging {

  import Tcp._

  implicit def system = context.system

  IO(Tcp) ! Bind(self, local)

  def receive: Receive = {
    case _: Bound ⇒
      log.info("Bound to {}", sender)
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
      val handler = context.actorOf(Props(new TsdbTelnetHandler(init, connection, kafka, system.settings.config, metrics))
        .withDeploy(Deploy.local))
      val pipeline = context.actorOf(TcpPipelineHandler.props(
        init, connection, handler).withDeploy(Deploy.local))

      connection ! Tcp.Register(pipeline, keepOpenOnPeerClosed = true)
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
    val hostport = config.getString("legacy.tsdb.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    system.actorOf(Props(new TsdbTelnetServer(addr, kafkaProducer, new TsdbTelnetMetrics(metricRegistry))))
  }
}
