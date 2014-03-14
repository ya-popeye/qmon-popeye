package popeye.pipeline

import akka.actor._
import akka.actor.SupervisorStrategy.Restart
import com.typesafe.config.{ConfigFactory, Config}
import popeye.proto.{Message, PackedPoints}
import popeye.IdGenerator
import akka.actor.OneForOneStrategy
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.Random
import java.util.concurrent.{Executors, TimeUnit}

object PointsDispatcherPerfTest {

  val pointsSample = {
    val attributes = Seq("host" -> "100", "cluster" -> "200", "dc" -> "5").map {
      case (name, value) => Message.Attribute.newBuilder().setName(name).setValue(value).build()
    }
    val builder = Message.Point.newBuilder()
      .setTimestamp(1)
      .setIntValue(1)
      .setMetric("points.disp.perf.test")
    for (attr <- attributes) {
      builder.addAttributes(attr)
    }
    val point = builder.build()
    val messagesSeq = Seq.fill(2000)(point)
    PackedPoints(messagesSeq)
  }

  class TestWorker(val batcher: PointsDispatcherPerfTestActor) extends PointsDispatcherWorkerActor {
    val random = new Random()

    override type Batcher = PointsDispatcherPerfTestActor

    override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
      case _ => Restart
    }

    val timing = Vector(0.0 -> 40, 0.2 -> 100, 0.4 -> 300, 0.5 -> 770, 0.75 -> 950, 0.95 -> 1100, 0.98 -> 1300, 0.99 -> 1400, 1.0001 -> 1800)

    private def sendTime(batchSize: Int) = {
      val rnd = random.nextDouble()
      val (key1, value1) = timing(timing.lastIndexWhere {case (key, _) => rnd > key})
      val (key2, value2) = timing.dropWhile {case (key, _) => rnd > key}.head

      val nominalTime = value1 + (rnd - key1) / (key2 - key1) * (value2 - value1)
      ((batchSize.toDouble / 10000) * nominalTime + 10).toInt / 3
    }

    override def processBatch(batchId: Long, buffer: Seq[PackedPoints]): Unit = {
      val batchSize = buffer.map(_.pointsCount).sum
      Thread.sleep(sendTime(batchSize))
      batcher.metrics.pointsMeter.mark(batchSize)
    }
  }

  class PointsDispatcherPerfTestActor(dispatcherConfig: Config,
                                      akkaDispatcher: String,
                                      val metrics: PointsDispatcherTestMetrics) extends PointsDispatcherActor {


    type Config = PointsDispatcherConfig
    type Metrics = PointsDispatcherTestMetrics
    val config = new PointsDispatcherConfig(dispatcherConfig)

    override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
      case _ ⇒ Restart
    }

    private var idx = 0

    def spawnWorker(): ActorRef = {
      idx += 1
      context.actorOf(
        Props.apply(new TestWorker(this)).withDispatcher(akkaDispatcher).withDeploy(Deploy.local),
        "points-sender-" + idx)
    }

    override val idGenerator: IdGenerator = new IdGenerator(0)
  }

  class PointsDispatcherTestMetrics(prefix: String, metricRegistry: MetricRegistry)
    extends PointsDispatcherMetrics(prefix, metricRegistry) {
  }

  def main(args: Array[String]) {
    val dispatcherConfig = ConfigFactory.parseString(
      """
        |max-queued = 150000
        |high-watermark = 10000
        |low-watermark = 1000
        |tick = 500ms
        |workers = 12
      """.stripMargin)
    val akkaConfig = ConfigFactory.parseString(
      """
        |pinned {
        |  type = "PinnedDispatcher"
        |  executor = "thread-pool-executor"
        |  thread-pool-executor.allow-core-pool-timeout = off
        |}
      """.stripMargin)
    val akkaDispatcher = "pinned"
    val metricRegistry = new MetricRegistry()
    val commitTime = metricRegistry.timer("commit-time")
    val metrics = new PointsDispatcherTestMetrics("perf-test", metricRegistry) {

    }
    val system = ActorSystem("test", akkaConfig)
    val dispatcherProps = Props(new PointsDispatcherPerfTestActor(dispatcherConfig, akkaDispatcher, metrics))
    val dispatcher = system.actorOf(dispatcherProps)
    implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

    def startSendingPoints(dispatcher: ActorRef): Unit = {
      val context = commitTime.time()
      val p = Promise[Long]()
      dispatcher ! DispatcherProtocol.Pending(Some(p))(Seq(pointsSample))
      p.future.map {
        _ =>
          context.stop()
          startSendingPoints(dispatcher)
      }
    }

    val consoleReporter = ConsoleReporter
      .forRegistry(metricRegistry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build()
    consoleReporter.start(1, TimeUnit.SECONDS)

    for (i <- 0 until 4000) {
      startSendingPoints(dispatcher)
    }
    system.awaitTermination()
    println("end")
  }
}
