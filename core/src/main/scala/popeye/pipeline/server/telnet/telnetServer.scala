package popeye.pipeline.server.telnet

import akka.actor._
import akka.io.IO
import akka.io._
import akka.pattern.AskTimeoutException
import akka.util.{Timeout, ByteString}
import com.codahale.metrics.{Timer, MetricRegistry}
import com.typesafe.config.Config
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import popeye.pipeline.DispatcherProtocol.Done
import popeye.pipeline.{PipelineChannel, PipelineSourceFactory, PipelineChannelWriter}
import popeye.proto.{Message, PackedPoints}
import popeye.{Logging, Instrumented}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Success, Failure}

class TelnetPointsMetrics(prefix: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val requestTimer = metrics.timer(s"$prefix.request.time")
  val commitTimer = metrics.timer(s"$prefix.commit.time")
  val pointsRcvMeter = metrics.meter(s"$prefix.points.received")
  val batchesCommitMeter = metrics.meter(s"$prefix.batches.commited")
  val batchSizes = metrics.histogram(s"$prefix.batches.size")
  val serverReadingSuspensions = metrics.meter(s"$prefix.server.reading.suspensions")
  val connections = new AtomicInteger(0)
  val connectionsGauge = metrics.gauge(s"$prefix.connections") {
    connections.get()
  }
  val preprocessingErrors = metrics.meter(s"$prefix.preprocessing-errors")
}

class TelnetPointsServerConfig(config: Config, val shardAttributes: Set[String]) {
  val hwPendingPoints: Int = config.getInt("high-watermark")
  val lwPendingPoints: Int = config.getInt("low-watermark")
  val batchSize: Int = config.getInt("batchSize")
  implicit val askTimeout: Timeout = new Timeout(config.getMilliseconds("produce.timeout"), MILLISECONDS)

  require(hwPendingPoints > lwPendingPoints, "High watermark should be greater then low watermark")
}

class TelnetPointsHandler(connection: ActorRef,
                          channelWriter: PipelineChannelWriter,
                          config: TelnetPointsServerConfig,
                          metrics: TelnetPointsMetrics)
  extends Actor with Logging {


  context watch connection

  type PointId = Long
  type BatchId = Long
  type CorrelationId = Long

  sealed case class TryReadResumeMessage(timestampMillis: Long = System.currentTimeMillis(), resume: Boolean = false)

  sealed case class CommitReq(sender: ActorRef, pointId: PointId,
                              correlation: CorrelationId,
                              timerContext: Timer.Context)

  private var bufferedPoints = PackedPoints()
  private var pendingCommits: Seq[CommitReq] = Vector()
  private val pendingCorrelations = mutable.TreeSet[PointId]()

  private val commands = {
    val commandListener = new CommandListener {

      override def addPoint(point: Message.Point): Unit = {
        bufferedPoints += point
        if (bufferedPoints.pointsCount >= config.batchSize) {
          sendPack()
        }
      }

      override def startExit(): Unit = {
        pendingExit = true
        debug(s"Triggered exit")
      }

      override def commit(correlationId: Option[Long]): Unit = {
        sendPack()
        if (correlationId.isDefined) {
          debug(s"Triggered commit for correlationId $correlationId and pointId $pointId")
          pendingCommits = (pendingCommits :+ CommitReq(sender, pointId, correlationId.get,
            metrics.commitTimer.timerContext())).sortBy(_.pointId)
        }
      }
    }
    new TelnetCommands(metrics, commandListener, config.shardAttributes)
  }

  @volatile
  private var pendingExit = false
  @volatile
  private var lastBatchId: BatchId = 0
  @volatile
  private var pointId: PointId = 0
  @volatile
  private var paused = false

  private val requestTimer = metrics.requestTimer.timerContext()

  override def postStop() {
    super.postStop()
    requestTimer.close()
    commands.close()
  }

  final def receive = {
    case Tcp.Received(data: ByteString) if data.length > 0 =>
      try {
        commands.process(data)
      } catch {
        case ex: Exception =>
          error(s"Err: ${ex.getMessage}", ex)
          sender ! Tcp.Write(ByteString(s"ERR ${ex.getClass.getSimpleName} ${ex.getMessage} \n"))
          context.stop(self)
      }
      tryReplyOk()
      throttle()

    case Done(completeCorrelationId, batchId) =>
      if (lastBatchId < batchId) {
        lastBatchId = batchId
      }
      debug(s"Produce done: ${completeCorrelationId.size} correlations " +
        s"as batch $batchId (now lastBatchId=$lastBatchId)")

      pendingCorrelations --= completeCorrelationId
      val commitedSize: Long = completeCorrelationId.size
      metrics.batchesCommitMeter.mark(commitedSize)
      tryReplyOk()
      throttle()

    case r: ReceiveTimeout =>
      throttle(timeout = true)

    case x: Tcp.ConnectionClosed =>
      debug(s"Connection closed $x")
      metrics.connections.decrementAndGet()
      context.stop(self)
  }

  def dumpErrors(): Unit = {
    val errors = commands.getErrorCounts()
    if (errors.nonEmpty) {
      warn(s"there was errors: $errors")
      metrics.preprocessingErrors.mark(errors.values.sum)
    }
  }

  private def sendPack() {
    import context.dispatcher
    dumpErrors()
    if (bufferedPoints.pointsCount > 0) {
      pointId += 1
      val p = Promise[Long]()
      metrics.batchSizes.update(bufferedPoints.pointsCount)
      channelWriter.write(Some(p), bufferedPoints)
      bufferedPoints = PackedPoints(sizeHint = config.batchSize + 1)
      pendingCorrelations.add(pointId)
      val timer = context.system.scheduler.scheduleOnce(config.askTimeout.duration, new Runnable {
        def run() {
          p.tryFailure(new AskTimeoutException("Producer timeout"))
        }
      })
      val cId = Seq(pointId)
      val ctx = context
      val me = self
      p.future onComplete {
        case Success(l) =>
          timer.cancel()
          me ! Done(cId, l)
        case Failure(ex) =>
          timer.cancel()
          connection ! Tcp.Write(ByteString("ERR Command processing timeout\n"))
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
              debug(s"Commit done: ${p.correlation} = $lastBatchId")
              p.sender ! Tcp.Write(ByteString(s"OK ${p.correlation} = $lastBatchId\n"))
              p.timerContext.stop()
          }
          pendingCommits = incomplete
      }
    }

    if (pendingExit && pendingCommits.isEmpty) {
      connection ! Tcp.Close
    }
  }

  def throttle(timeout: Boolean = false) {
    val size: Int = pendingCorrelations.size
    if (size > config.hwPendingPoints) {
      if (!paused) {
        paused = true
        debug(s"Pausing reads: $size > ${config.hwPendingPoints}")
        context.setReceiveTimeout(1 millisecond)
      }
      metrics.serverReadingSuspensions.mark()
      connection ! Tcp.SuspendReading
    }

    // resume reading only after recieving 'end-of-queue' marker
    if (paused && timeout && size < config.lwPendingPoints) {
      paused = false
      connection ! Tcp.ResumeReading
      context.setReceiveTimeout(Duration.Undefined)
      debug(s"Reads resumed: $size < ${config.lwPendingPoints}")
    }
  }
}

class TelnetPointsServer(config: TelnetPointsServerConfig,
                         local: InetSocketAddress,
                         channelWriter: PipelineChannelWriter,
                         metrics: TelnetPointsMetrics)
  extends Actor with Logging {

  import Tcp._

  implicit def system = context.system

  IO(Tcp) ! Bind(self, local)

  def receive: Receive = {
    case _: Bound ⇒
      info("Bound to $sender")
      context.become(bound(sender))
  }

  def bound(listener: ActorRef): Receive = {
    case Connected(remote, _) ⇒
      val connection = sender

      val handler = context.actorOf(Props.apply(
        new TelnetPointsHandler(connection, channelWriter, config, metrics)
      ).withDeploy(Deploy.local))

      debug(s"Connection from $remote (connection=${connection.path})")
      metrics.connections.incrementAndGet()
      connection ! Tcp.Register(handler, keepOpenOnPeerClosed = true)
  }

  override def preStart() {
    super.preStart()
    info("Started Tsdb Telnet server")
  }

  override def postStop() {
    super.postStop()
    info("Stoped Tsdb Telnet server")
  }
}

object TelnetPointsServer {

  def sourceFactory(shardAttributes: Set[String]): PipelineSourceFactory = {
    new PipelineSourceFactory {
      def startSource(sinkName: String, channel: PipelineChannel,
                      config: Config, ect: ExecutionContext): Unit = {
        channel.actorSystem.actorOf(props(sinkName, config, shardAttributes, channel.newWriter(), channel.metrics))
      }
    }
  }

  def props(prefix: String,
            config: Config,
            shardAttributes: Set[String],
            channelWriter: PipelineChannelWriter,
            metricRegistry: MetricRegistry): Props = {
    val host = config.getString("listen")
    val port = config.getInt("port")
    val addr = new InetSocketAddress(host, port)
    val serverConf = new TelnetPointsServerConfig(config, shardAttributes)
    Props.apply(new TelnetPointsServer(serverConf, addr, channelWriter,
      new TelnetPointsMetrics(prefix, metricRegistry)))
  }
}
