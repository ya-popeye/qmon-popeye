package popeye.pipeline

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, Props, ActorRef}
import akka.testkit.TestActorRef
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigValueFactory, ConfigFactory, Config}
import popeye.test.AkkaTestKitSpec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import akka.pattern.AskTimeoutException

/**
 * @author Andrey Stepachev
 */
class DispatcherActorSpec extends AkkaTestKitSpec("DispatcherActor") {

  val registry = new MetricRegistry
  implicit val sch = system.scheduler
  implicit val ectx = system.dispatcher

  behavior of "Dispatcher"

  it should "buffer" in {
    val metrics = new DispatcherMetrics("test", registry)
    val dconf = new DispatcherConfig(config(tick = 10000))
    val actor: TestActorRef[MyDispatcherActor] = TestActorRef(
      Props.apply(new MyDispatcherActor(dconf, metrics, { dispatcher =>
        new MyDispatcherWorker(dispatcher)
      })))
    intercept[AskTimeoutException] {
      // here we wait for tick. We've pushed less then low-watermark,
      // with bit tick we will wait too long and fail.
      Await.result(DispatcherActor.sendBatch(actor, 1, Seq("ab", "bc", "cd"), 1 seconds), 5 seconds)
    }
    // and now, we push enough data to overcome low-watermark
    Await.result(DispatcherActor.sendBatch(actor, 1, Seq("uu", "uu", "uu"), 1 seconds), 2 seconds)
  }

  it should "tolerate worker failures" in {
    val metrics = new DispatcherMetrics("test", registry)
    val dconf = new DispatcherConfig(config(tick = 10, highWatermark = 1, lowWatermark = 0))
    val numberOfFails = 10
    val failsCounter = new AtomicInteger(numberOfFails)
    val actor: TestActorRef[MyDispatcherActor] = TestActorRef(
      Props.apply(new MyDispatcherActor(dconf, metrics, { dispatcher =>
        new FailWorker(dispatcher, failsCounter)
      })))
    for (_ <- 0 until numberOfFails) {
      Await.ready(DispatcherActor.sendBatch(actor, 1, Seq("uu"), 1 seconds), 5 seconds)
    }
    Await.result(DispatcherActor.sendBatch(actor, 1, Seq("uu"), 1 seconds), 5 seconds)
  }

  it should "not duplicate actor refs in work queue on worker failure" in {
    val metrics = new DispatcherMetrics("test", registry)
    val dconf = new DispatcherConfig(config())
    val numberOfFails = new AtomicInteger(10000)
    val actor: TestActorRef[MyDispatcherActor] = TestActorRef(
      Props.apply(new MyDispatcherActor(dconf, metrics, { dispatcher =>
        new FailWorker(dispatcher, numberOfFails)
      })))
    for (_ <- 0 to 1000) {
      DispatcherActor.sendBatch(actor, 1, Seq("uu", "uu", "uu"), 1 seconds)
    }
    actor.underlyingActor.workQueue.size should (equal(0) or equal(1))
  }

  def config(tick: Int = 100,
             highWatermark: Int = 100,
             lowWatermark: Int = 5): Config = ConfigFactory.parseString(
    s"""
    | tick = ${ tick }ms
    | max-queued=1000
    | workers = 1
    | high-watermark = $highWatermark
    | low-watermark = $lowWatermark
      """.stripMargin)
    .withFallback(ConfigFactory.parseResources("reference.conf"))
    .resolve()

}

class FailWorker(val batcher: MyDispatcherActor, numberOfFails: AtomicInteger) extends WorkerActor {
  type Batcher = MyDispatcherActor
  type Batch = Seq[String]

  override def processBatch(batchId: Long, pack: Batch): Unit = {
    if (numberOfFails.decrementAndGet() >= 0) {
      throw new RuntimeException("fail")
    }
  }
}

class MyDispatcherWorker(val batcher: MyDispatcherActor) extends WorkerActor {
  type Batcher = MyDispatcherActor
  type Batch = Seq[String]

  val batches = mutable.HashMap[Long, Seq[String]]()

  def processBatch(batchId: Long, pack: Seq[String]): Unit = {
    batches.put(batchId, pack)
  }
}

class MyDispatcherActor(val config: DispatcherConfig,
                        val metrics: DispatcherMetrics,
                        val workerFactory: (MyDispatcherActor) => Actor)
  extends DispatcherActor {

  type Config = DispatcherConfig
  type Metrics = DispatcherMetrics
  type Batch = Seq[String]

  type PromiseOffset = (Long, Promise[Long])

  var batchId: Long = 1
  var batches = new ArrayBuffer[String]()
  var promises = new ArrayBuffer[PromiseOffset]()
  var consumed: Long = 0
  var added: Long = 0

  def spawnWorker(): ActorRef = {
    context.system.actorOf(Props.apply(workerFactory(this)))
  }

  def buffer(buffer: Seq[String], batchIdPromise: Option[Promise[Long]]): Unit = {
    val prevLen = batches.size
    batches ++= buffer
    added += batches.size - prevLen
    if (batchIdPromise.isDefined)
      promises += Pair(added, batchIdPromise.get)
  }

  def unbuffer(ignoreMinSize: Boolean): Option[WorkerData] = {
    if (batches.isEmpty || !(ignoreMinSize || batches.size >= config.lowWatermark))
      None
    else {
      val data: (Seq[Promise[Long]], Seq[String]) =
        if (ignoreMinSize || batches.size <= config.highWatermark) {
          val r = batches
          batches = new ArrayBuffer[String]()
          val p = promises
          promises = new ArrayBuffer[PromiseOffset]
          consumed += batches.size
          (p.map(_._2), r)
        } else {
          val endIdx = Math.min(config.highWatermark, batches.length)
          val r = batches.slice(0, endIdx)
          batches = batches.drop(endIdx)
          val (p, left) = promises.partition(_._1 >= consumed)
          promises = left
          (p.map(_._2), r)
        }
      batchId += 1
      Some(new WorkerData(batchId, data._2, data._1))
    }
  }
}
