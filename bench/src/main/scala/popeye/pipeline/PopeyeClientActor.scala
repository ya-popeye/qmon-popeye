package popeye.pipeline

import akka.actor.{ActorSystem, Actor, ActorRef, Props}
import akka.io.{IO, Tcp}
import akka.util.{ByteString, Timeout}
import com.codahale.metrics._
import java.io.{InputStreamReader, BufferedReader, File}
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import popeye.pipeline.PopeyeClientActor._

import scala.util.Random

class PopeyeClientActor(remote: InetSocketAddress, hostIndex: Int, pointGen: PointGen) extends Actor {

  import Tcp._
  import context.system

  val period = 1 second

  val random: () => Int = {
    var num: Long = hostIndex
    () => {
      num = (num * 1231241223301l) % 4000037
      num.toInt
    }
  }

  var commitNumber = random()
  var currentBatchPoints = 0
  var commitContext: Timer.Context = null

  def receive = {
    case Start =>
      IO(Tcp) ! Connect(remote)

    case CommandFailed(_: Connect) =>
      println("initial connection failed")
      context stop self

    case c@Connected(remote, local) =>
      PopeyeClientActor.connections.inc()
      val connection = sender
      connection ! Register(self)
      context become metricsSender(connection)
      self ! SendNewPoints
  }

  def metricsSender(connection: ActorRef): Actor.Receive = {
    def reconnect() {
      connection ! Close
      IO(Tcp) ! Connect(remote)
      context unbecome()
      commitContext.close()
      PopeyeClientActor.connections.dec()
    }

    {
      case SendNewPoints =>
        val (pointsString, numberOfPoints) = pointGen.pointsString(hostIndex)
        currentBatchPoints = numberOfPoints
        val byteString = pointsString ++ ByteString(f"commit $commitNumber\n")
        connection ! Write(byteString)
        PopeyeClientActor.bytesMetric.mark(byteString.size)
        commitContext = PopeyeClientActor.commitTimeMetric.time()
      case Received(data) =>
        val string = data.utf8String
        if (string.startsWith("OK")) {
          commitContext.stop()
          val stringWithoutOK = string.substring(3)
          val oldCommitNumber = stringWithoutOK.substring(0, stringWithoutOK.indexOf(' ')).toInt
          if (oldCommitNumber != commitNumber) {
            println(f"actorId:$hostIndex wrong commit number:$oldCommitNumber, should be $commitNumber")
          }
          commitNumber = random()
          self ! SendNewPoints
          PopeyeClientActor.pointsMetric.mark(currentBatchPoints)
          currentBatchPoints = 0
          PopeyeClientActor.successfulCommits.mark()
        } else if (string.startsWith("ERR")) {
          PopeyeClientActor.failedCommits.mark()
          reconnect()
        } else {
          println(f"strange response: $string")
          reconnect()
        }
      case CommandFailed(w: Write) =>
        println(f"actorId:$hostIndex write command failed $w, reconnecting")
        PopeyeClientActor.ioFails.inc()
        reconnect()
      case CloseConnection =>
        PopeyeClientActor.connections.dec()
        connection ! Close
    }
  }

}

object PopeyeClientActor {

  case object SendNewPoints

  case object Start

  case object CloseConnection

  val pointsSent = new AtomicInteger(0)
  val bytesSent = new AtomicInteger(0)
  val metrics = new MetricRegistry()
  val pointsMetric = metrics.meter("points")
  val bytesMetric = metrics.meter("bytes")
  val commitTimeMetric = metrics.timer("commit-time")
  val successfulCommits = metrics.meter("commits")
  val failedCommits = metrics.meter("failed-commits")
  val ioFails = metrics.counter("io-fails")
  val connections = metrics.counter("connections")

  def props(remote: InetSocketAddress, id: Int, pointsGen: PointGen) =
    Props(new PopeyeClientActor(remote, id, pointsGen))

  def main(args: Array[String]) {
    implicit val timeout = Timeout(5 seconds)
    val (optionalParams, mandatoryParams) = args.partition(_.startsWith("--"))
    val nHosts = mandatoryParams(0).toInt
    val nBatches = mandatoryParams(1).toInt
    val address = {
      val List(host, port) = mandatoryParams(2).split(":").toList
      new InetSocketAddress(InetAddress.getByName(host), port.toInt)
    }
    val metricsDir = mandatoryParams(3)
    val isInteractive = optionalParams.contains("--interactive")
    val isSingleMetric = optionalParams.contains("--one_metric")
    val pointGen = if (isSingleMetric) {
      new SingleMetricPointGen(nBatches * MetricGenerator.metrics.size)
    } else {
      new AllMetricsPointGen(nBatches)
    }
    val system = ActorSystem()
    //    val randomTags = optionalParams.find(_.contains("--random")).map(str => str.split("_")(1).toInt).getOrElse(0)
    val actors = (1 to nHosts).map(i => system.actorOf(PopeyeClientActor.props(address, i, pointGen)))

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
      .build(new File(metricsDir))

    csvReporter.start(5, TimeUnit.SECONDS)

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        for (actor <- actors) {
          actor ! CloseConnection
        }
        system.shutdown()
        system.awaitTermination(5 seconds)
      }
    }))

    for (actor <- actors) {
      Thread.sleep(10)
      actor ! Start
    }
    var hosts = nHosts
    val inReader = new BufferedReader(new InputStreamReader(System.in))
    if (isInteractive) while(true) {
      inReader.readLine()
      val oldHosts = hosts + 1
      hosts += 100
      val actors = (oldHosts to hosts).map(i => system.actorOf(PopeyeClientActor.props(address, i, pointGen)))
      for (actor <- actors) {
        Thread.sleep(10)
        actor ! Start
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
