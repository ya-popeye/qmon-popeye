package popeye.pipeline

import akka.actor._
import akka.io.{IO, Tcp}
import akka.util.{ByteString, Timeout}
import com.codahale.metrics._
import java.io.{InputStreamReader, BufferedReader, File}
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import popeye.pipeline.PopeyeClientActor._

import scala.util.Random

class PopeyeClientActor(remote: InetSocketAddress,
                        hostIndex: Int,
                        pointGen: PointGen,
                        connectionTimeout: FiniteDuration,
                        commitTimeout: FiniteDuration,
                        sendDelay: FiniteDuration) extends FSM[ClientState, ClientData] {

  import Tcp._
  import context.system

  var corrNum = 0

  def nextCorrelationNumber: Int = {
    corrNum += 1
    corrNum
  }

  val random = new Random()

  startWith(IdleState, IdleData)

  when(IdleState) {
    case Event(StartClient, state) =>
      IO(Tcp) ! Connect(remote, timeout = Some(connectionTimeout))
      goto(Connecting)
  }

  when(Connecting) {
    case Event(c: Connected, IdleData) =>
      connections.inc()
      val connection = sender
      connection ! Register(self)
      goto(Sending) using ConnectionData(connection)

    case Event(CommandFailed(_: Connect), IdleData) =>
      connectFailedMeter.mark()
      self ! StartClient
      goto(IdleState) using IdleData
  }

  when(Sending, stateTimeout = (sendDelay.toNanos / 10) nanos) {
    case Event(StateTimeout, ConnectionData(connection)) =>
      if (random.nextInt(10) == 0) {
        val (pointsString, numberOfPoints) = pointGen.pointsString(hostIndex)
        val correlationNumber = nextCorrelationNumber
        val byteString = pointsString ++ ByteString(f"commit $correlationNumber\n")
        connection ! Write(byteString)
        PopeyeClientActor.bytesMetric.mark(byteString.size)
        val commitContext = PopeyeClientActor.commitTimeMetric.time()
        goto(WaitingForAck) using CommitData(connection, correlationNumber, numberOfPoints, commitContext)
      } else stay()
  }

  when(WaitingForAck, stateTimeout = commitTimeout) {
    case Event(Received(data), CommitData(connection, correlationNumber, numberOfPoints, commitContext)) =>
      val string = data.utf8String
      if (string.startsWith("OK")) {
        commitContext.stop()
        val stringWithoutOK = string.substring(3)
        val oldCommitNumber = stringWithoutOK.substring(0, stringWithoutOK.indexOf(' ')).toInt
        if (oldCommitNumber != correlationNumber) {
          println(f"actorId:$hostIndex wrong commit number:$oldCommitNumber, should be $correlationNumber")
        }
        PopeyeClientActor.pointsMetric.mark(numberOfPoints)
        PopeyeClientActor.successfulCommits.mark()
        goto(Sending) using ConnectionData(connection)
      } else if (string.startsWith("ERR")) {
        PopeyeClientActor.failedCommits.mark()
        reconnect(connection)
      } else {
        println(f"strange response: $string")
        reconnect(connection)
      }
    case Event(CommandFailed(w: Write), CommitData(connection, _, _, _)) =>
      ioFails.mark()
      reconnect(connection)
    case Event(StateTimeout, CommitData(connection, _, _, _)) =>
      commitTimeoutMeter.mark()
      reconnect(connection)
  }

  def reconnect(connection: ActorRef) = {
    closeConnection(connection)
    self ! StartClient
    goto(IdleState) using IdleData
  }

  whenUnhandled {
    case Event(StopClient, _) =>
      stop()
  }

  onTermination {
    case StopEvent(_, _, ConnectionData(connection)) =>
      closeConnection(connection)

    case StopEvent(_, _, CommitData(connection, _, _, _)) =>
      closeConnection(connection)
  }

  def closeConnection(connection: ActorRef) {
    connections.dec()
    connection ! Close
  }
}

object PopeyeClientActor {

  sealed trait ClientState

  case object IdleState extends ClientState

  case object Connecting extends ClientState

  case object WaitingForAck extends ClientState

  case object Sending extends ClientState

  sealed trait ClientData

  case object IdleData extends ClientData

  case class ConnectionData(connection: ActorRef) extends ClientData

  case class CommitData(connection: ActorRef,
                        correlationNumber: Int,
                        numberOfPoints: Int,
                        timerContext: Timer.Context) extends ClientData

  case object StartClient

  case object StopClient

  case class Config(address: InetSocketAddress = new InetSocketAddress(InetAddress.getLocalHost, 4444),
                    batches: Int = 10,
                    connections: Int = 4000,
                    sendDelay: FiniteDuration = 0 millis,
                    connectionTimeout: FiniteDuration = 30 seconds,
                    commitTimeout: FiniteDuration = 30 seconds,
                    startInterval: FiniteDuration = 10 millis,
                    metricsDir: File = new File("load-metrics"),
                    isInteractive: Boolean = false,
                    isSingleMetricMode: Boolean = false)

  val metrics = new MetricRegistry()
  val pointsMetric = metrics.meter("points")
  val bytesMetric = metrics.meter("bytes")
  val commitTimeMetric = metrics.timer("commit.time")
  val successfulCommits = metrics.meter("commits")
  val failedCommits = metrics.meter("failed.commits")
  val ioFails = metrics.meter("io.failures")
  val connections = metrics.counter("connections")
  val commitTimeoutMeter = metrics.meter("commit.timeouts")
  val connectFailedMeter = metrics.meter("connect.failures")

  val parser = new scopt.OptionParser[Config]("popeye-run-class.sh popeye.pipeline.PopeyeClientActor") {
    head("popeye load test tool", "0.1")
    opt[String]("address") valueName "<slicer address>" action {
      (param, config) =>
        val List(host, port) = param.split(":").toList
        config.copy(address = new InetSocketAddress(InetAddress.getByName(host), port.toInt))
    }
    opt[Int]("connections") valueName "<connections>" action {
      (param, config) => config.copy(connections = param)
    }
    opt[Int]("batches") valueName "<batches>" action {
      (param, config) => config.copy(batches = param)
    }
    opt[Int]("send-delay") valueName "<send delay millis>" action {
      (param, config) => config.copy(sendDelay = FiniteDuration(param, MILLISECONDS))
    }
    opt[Int]("connect-timeout") valueName "<connection timeout in seconds>" action {
      (param, config) => config.copy(connectionTimeout = FiniteDuration(param, SECONDS))
    }
    opt[Int]("commit-timeout") valueName "<commit timeout in seconds>" action {
      (param, config) => config.copy(commitTimeout = FiniteDuration(param, SECONDS))
    }
    opt[Unit]("interactive") action {
      (_, config) => config.copy(isInteractive = true)
    }
    opt[Unit]("one-metric") action {
      (_, config) => config.copy(isSingleMetricMode = true)
    }
    opt[Int]("start-interval") valueName "<clients start interval millis>" action {
      (param, config) => config.copy(startInterval = FiniteDuration(param, MILLISECONDS))
    }
  }

  def props(config: Config, id: Int, pointsGen: PointGen) =
    Props(
      new PopeyeClientActor(
        config.address,
        id,
        pointsGen,
        config.connectionTimeout,
        config.commitTimeout,
        config.sendDelay
      )
    )

  def main(args: Array[String]) {
    implicit val timeout = Timeout(5 seconds)
    val config = parser.parse(args, Config()).get
    val pointGen = if (config.isSingleMetricMode) {
      new SingleMetricPointGen(config.batches * MetricGenerator.metrics.size)
    } else {
      new AllMetricsPointGen(config.batches)
    }
    val system = ActorSystem()
    val actors = (1 to config.connections).map(i => system.actorOf(PopeyeClientActor.props(config, i, pointGen)))

    val consoleReporter = ConsoleReporter
      .forRegistry(PopeyeClientActor.metrics)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build()
    consoleReporter.start(1, TimeUnit.SECONDS)

    val csvReporter = CsvReporter
      .forRegistry(PopeyeClientActor.metrics)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(config.metricsDir)

    csvReporter.start(5, TimeUnit.SECONDS)

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        for (actor <- actors) {
          actor ! StopClient
        }
        system.shutdown()
        system.awaitTermination(5 seconds)
      }
    }))

    for (actor <- actors) {
      Thread.sleep(config.startInterval.toMillis)
      actor ! StartClient
    }
    if (config.isInteractive) {
      var hosts = config.connections
      val inReader = new BufferedReader(new InputStreamReader(System.in))
      while(true) {
        inReader.readLine()
        val oldHosts = hosts + 1
        hosts += 100
        val actors = (oldHosts to hosts).map(i => system.actorOf(PopeyeClientActor.props(config, i, pointGen)))
        for (actor <- actors) {
          Thread.sleep(config.startInterval.toMillis)
          actor ! StartClient
        }
      }
    }

  }
}

trait PointGen {
  def pointsString(hostIndex: Int): (ByteString, Int)
}

class AllMetricsPointGen(nBatches: Int) extends PointGen {
  val random = new Random

  override def pointsString(hostIndex: Int): (ByteString, Int) = {
    val timestamp = (System.currentTimeMillis() / 1000).toInt
    val strings = for (metric <- MetricGenerator.metrics; _ <- 1 to nBatches) yield {
      MetricGenerator.pointStringWithoutTags(metric, timestamp, random.nextInt()).append(' ')
        .append("dc=dc_").append(hostIndex % 10).append(' ')
        .append("host=host_").append(hostIndex).append(' ')
        .append("cluster=popeye_test")
        .append('\n')
        .toString()
    }
    (ByteString(strings.mkString), strings.size)
  }
}

class SingleMetricPointGen(nPoints: Int) extends PointGen {
  val random = new Random

  override def pointsString(hostIndex: Int): (ByteString, Int) = {
    val timestamp = (System.currentTimeMillis() / 1000).toInt
    val strings = for (i <- 0 until nPoints) yield {
      MetricGenerator.pointStringWithoutTags("the_metric", timestamp, random.nextInt()).append(' ')
        .append("host=").append(hostIndex).append(' ')
        .append("the_tag=").append(i).append(' ')
        .append("cluster=popeye_test")
        .append('\n')
        .toString()
    }
    (ByteString(strings.mkString), strings.size)
  }
}
