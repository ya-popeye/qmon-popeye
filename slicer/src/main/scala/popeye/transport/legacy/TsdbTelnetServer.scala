package popeye.transport.legacy

import akka.actor._
import akka.io._
import akka.util.{Timeout, ByteString}
import akka.io.IO
import akka.io.TcpPipelineHandler.{WithinActorContext, Init}
import java.net.InetSocketAddress
import net.opentsdb.core.Tags
import popeye.transport.kafka.{ProduceNeedThrottle, ProduceDone, ProducePending}
import popeye.transport.proto.Message.{Attribute, Point}
import scala.collection.mutable
import com.codahale.metrics.{Timer, MetricRegistry}
import com.typesafe.config.Config
import popeye.Instrumented
import popeye.transport.proto.PackedPoints
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import akka.pattern.AskTimeoutException
import scala.annotation.tailrec
import popeye.transport.{CompressionDecoder, LineDecoder}
import popeye.transport.CompressionDecoder.{Snappy, Gzip}
import java.util.concurrent.atomic.AtomicInteger
import org.parboiled.common.Base64

class TsdbTelnetMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val requestTimer = metrics.timer("request-time")
  val commitTimer = metrics.timer("commit-time")
  val pointsRcvMeter = metrics.meter("points-received")
  val pointsCommitMeter = metrics.meter("points-commited")
  val connections = new AtomicInteger(0)
  val connectionsGauge = metrics.gauge("connections") {
    connections.get()
  }
}

class TsdbTelnetHandler(init: Init[WithinActorContext, ByteString, ByteString],
                        connection: ActorRef,
                        kafkaProducer: ActorRef,
                        config: Config,
                        metrics: TsdbTelnetMetrics)
  extends Actor with ActorLogging {

  // TODO: move to config
  val hwPendingPoints: Int = config.getInt("legacy.tsdb.high-watermark")
  val lwPendingPoints: Int = config.getInt("legacy.tsdb.low-watermark")
  val batchSize: Int = config.getInt("legacy.tsdb.batchSize")
  implicit val askTimeout: Timeout = 10 seconds

  require(hwPendingPoints > lwPendingPoints, "High watermark should be greater then low watermark")

  type PointId = Long
  type BatchId = Long
  type CorrelationId = Long

  sealed case class CommitReq(sender: ActorRef, pointId: PointId, correlation: CorrelationId, timerContext: Timer.Context)

  private var pendingCommits: Seq[CommitReq] = Vector()
  private val pendingCorrelations = mutable.TreeSet[PointId]()
  private var bufferedPoints = PackedPoints()
  @volatile
  private var pendingExit = false
  @volatile
  private var lastBatchId: BatchId = 0
  @volatile
  private var pointId: PointId = 0
  @volatile
  private var suspended = false

  private val requestTimer = metrics.requestTimer.timerContext()

  private var deflater: Option[CompressionDecoder] = None
  private val lineDecoder = new LineDecoder()
  private var bufferedLine: Option[ByteString] = None

  private val debugBuffer = ByteString.newBuilder

  override def postStop() {
    super.postStop()
    requestTimer.close()
    deflater foreach { _.close }
  }

  @tailrec
  private def tryParseCommand(input: ByteString): Option[ByteString] = {
    lineDecoder.tryParse(input) match {
      case (None, remainder) =>
        remainder
      case (Some(line), remainder) =>
        val strings = Tags.splitString(line.utf8String, ' ')
        strings(0) match {

          case "deflate" =>
            if (deflater.isDefined)
              throw new IllegalArgumentException("Already in deflate mode")
            deflater = Some(new CompressionDecoder(strings(1).toInt, Gzip()))
            if (log.isDebugEnabled)
              log.debug(s"Entering deflate mode, expected ${strings(1)} bytes")
            return remainder // early exit, we need to reenter doCommands

          case "snappy" =>
            if (deflater.isDefined)
              throw new IllegalArgumentException("Already in deflate mode")
            deflater = Some(new CompressionDecoder(strings(1).toInt, Snappy()))
            if (log.isDebugEnabled)
              log.debug(s"Entering snappy mode, expected ${strings(1)} bytes")
            return remainder // early exit, we need to reenter doCommands

          case "put" =>
            metrics.pointsRcvMeter.mark()
            bufferedPoints += parsePoint(strings)
            if (bufferedPoints.size >= batchSize) {
              sendPack()
            }

          case "commit" =>
            sendPack()
            if (log.isDebugEnabled)
              log.debug(s"Triggered commit for correlationId ${strings(1)} and pointId $pointId")
            pendingCommits = (pendingCommits :+ CommitReq(sender, pointId, strings(1).toLong,
              metrics.commitTimer.timerContext())).sortBy(_.pointId)

          case "ver" =>
            sendPack()
            sender ! init.Command(ByteString("OK unknown\n"))

          case "exit" =>
            sendPack()
            pendingExit = true
            if (log.isDebugEnabled)
              log.debug(s"Triggered exit")

          case c: String =>
            sender ! init.Command(ByteString(s"ERR Unknown command $c\n"))
            if (log.isDebugEnabled)
              log.debug(s"Unknown command ${c.take(50)}")
            context.stop(self)
        }
        remainder match {
          case Some(l) => tryParseCommand(l)
          case None =>
            None
        }
    }
  }

  @tailrec
  private def doCommands(input: ByteString): Option[ByteString] = {

    deflater match {
      case Some(decoder) =>
        val remainder = decoder.decode(input) {
          buf =>
            val parserRemainder = tryParseCommand(buf)
            parserRemainder foreach decoder.pushBack
        }
        if (decoder.isClosed) {
          deflater = None
          if (log.isDebugEnabled)
            log.debug(s"Leaving encoded ${decoder.codec} mode")
          if (remainder.isDefined)
            doCommands(remainder.get)
          else
            None
        } else {
          None
        }

      case None =>
        val parserRemainder = tryParseCommand(input)
        // we should ensure, that next iteration will no be empty,
        // otherwise we need to return remainder to calling context
        // expected to be buffered somewhere and refeed on next call
        // if decoder activated, reenter doCommands too
        if (deflater.isDefined && parserRemainder.isDefined) {
          doCommands(parserRemainder.get)
        } else {
          parserRemainder
        }
    }
  }

  final def receive = {
    case init.Event(data: ByteString) if data.length > 0 =>
      if (log.isDebugEnabled)
        debugBuffer.append(data)
      try {
        val concat = if (bufferedLine.isDefined) {
          bufferedLine.get ++ data
        } else {
          data
        }
        bufferedLine = doCommands(concat)
      } catch {
        case ex: Exception =>
          if (log.isDebugEnabled) {
            // log errors only if debug enabled
            log.error(ex, "Failed command base64:\n" + Base64.rfc2045().encodeToString(debugBuffer.result().toArray, true))
          }
          sender ! init.Command(ByteString("ERR " + ex.getMessage + "\n"))
          context.stop(self)
      }
      tryReplyOk()
      checkSuspension()

    case ProduceNeedThrottle =>
      connection ! Tcp.SuspendReading
      suspended = true

    case ProduceDone(completeCorrelationId, batchId) =>
      if (lastBatchId < batchId) {
        lastBatchId = batchId
      }
      if (log.isDebugEnabled)
        log.debug(s"Produce done: ${completeCorrelationId.size} correlations as batch $batchId (now lastBatchId=$lastBatchId)")

      pendingCorrelations --= completeCorrelationId
      val commitedSize: Long = completeCorrelationId.size
      metrics.pointsCommitMeter.mark(commitedSize)
      tryReplyOk()
      checkSuspension()

    case x: Tcp.ConnectionClosed =>
      if (log.isDebugEnabled)
        log.debug("Connection closed {}", x)
      context.stop(self)
  }

  private def sendPack() {
    import context.dispatcher
    if (!bufferedPoints.isEmpty) {
      pointId += 1
      val p = Promise[Long]()
      kafkaProducer ! ProducePending(Some(p))(bufferedPoints)
      bufferedPoints = new PackedPoints
      pendingCorrelations.add(pointId)
      val timer = context.system.scheduler.scheduleOnce(askTimeout.duration, new Runnable {
        def run() { p.tryFailure(new AskTimeoutException("Producer timeout")) }
      })
      val cId = Seq(pointId)
      val ctx = context
      val me = self
      p.future onComplete {
        case Success(l) =>
          timer.cancel()
          me ! ProduceDone(cId, l)
        case Failure(ex) =>
          timer.cancel()
          connection ! init.Command(ByteString("ERR Command processing timeout\n"))
          ctx.stop(me)
      }
    }
  }

  def tryReplyOk() {

    if (!pendingCommits.isEmpty) {
      val minPoint: Long = pendingCorrelations.headOption getOrElse Long.MaxValue
      pendingCommits.span(_.pointId < minPoint) match {
        case (complete, incomplete) =>
          complete foreach {
            p =>
              if (log.isDebugEnabled)
                log.debug(s"Commit done: ${p.correlation} = $lastBatchId")
              p.sender ! init.Command(ByteString(s"OK ${p.correlation} = $lastBatchId\n"))
              p.timerContext.stop()
          }
          pendingCommits = incomplete
      }
    }

    if (pendingExit && pendingCommits.isEmpty) {
      connection ! Tcp.Close
    }
  }

  def checkSuspension() {
    val size: Int = pendingCorrelations.size
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
    for (i <- startIdx until tags.length) {
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
        new TcpReadWriteAdapter >>
        new BackpressureBuffer(lowBytes = 1 * 1024 * 1024, highBytes = 4 * 1024 * 1024, maxBytes = 10 * 1024 * 1024))

      val connection = sender
      val handler = context.actorOf(Props(new TsdbTelnetHandler(init, connection, kafka, system.settings.config, metrics))
        .withDeploy(Deploy.local))
      val pipeline = context.actorOf(TcpPipelineHandler.props(
        init, connection, handler).withDeploy(Deploy.local))

      if (log.isDebugEnabled)
        log.debug("Connection from {}", remote)
      metrics.connections.incrementAndGet()
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
