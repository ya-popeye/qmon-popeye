package popeye.pipeline.kafka

import akka.actor.Props
import akka.testkit.TestActorRef
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import java.util.Random
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import popeye.pipeline.{PointsSink, PointsSource, AtomicList}
import popeye.test.{PopeyeTestUtils, MockitoStubs}
import PopeyeTestUtils._
import popeye.proto.{Message, PackedPoints}
import popeye.pipeline.test.AkkaTestKitSpec
import scala.concurrent.{ExecutionContext, Promise, Await}
import scala.concurrent.duration._
import popeye.pipeline.kafka.KafkaPointsConsumer.DropStrategy
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Executors
import scala.concurrent.Future
import popeye.proto.Message.Point
import popeye.IdGenerator


/**
 * @author Andrey Stepachev
 */
class KafkaPointsConsumerSpec extends AkkaTestKitSpec("KafkaPointsConsumer") with MockitoSugar with MockitoStubs with ShouldMatchers {

  implicit val sch = system.scheduler
  implicit val ectx = system.dispatcher
  implicit val rnd = new Random(1234)
  val idGenerator = new IdGenerator(1)

  "Dispatcher" should "buffer" in {
    val config = mkConfig()//.withValue("tick", ConfigValueFactory.fromAnyRef(10000))
    val events1 = 1l -> PopeyeTestUtils.mkEvents(3)
    val events2 = 2l -> PopeyeTestUtils.mkEvents(3)
    val source = createPointsSource(events1, events2)
    val testCompletion = Promise[Unit]()
    val listener = new MyListener(failBatches = Set(2))({
      me =>
      if (me.sinkedBatches.size == 1 && me.droppedBatches.size == 1) testCompletion.success(())
    })
    val noOpStrategy: PackedPoints => SendAndDrop = points => SendAndDrop(pointsToSend = points)
    createConsumer(config, source, listener, noOpStrategy)
    Await.result(testCompletion.future, 5 seconds)
  }

  def mkConfig(maxParallelSenders: Int = 1, tick: Int = 500): Config = ConfigFactory.parseString(
    s"""
    | topic = popeye-points
    | group = test
    | batch-size = 2
    | max-lag = 60s
    | max-parallel-senders = $maxParallelSenders
    | tick = ${tick}ms
    | zk.quorum = "localhost:2181"
    | broker.list = "localhost:9092"
      """.stripMargin)
    .withFallback(ConfigFactory.parseResources("reference.conf")
    .getConfig("common.popeye.pipeline.kafka.consumer"))
    .resolve()

  it should "use drop strategy" in {
    val config = mkConfig()
    val points = Seq(createPoint(metric = "send"), createPoint(metric = "drop"))
    val events1 = 1l -> points
    val events2 = 2l -> points
    val source = createPointsSource(events1, events2)
    val testCompletion = Promise[Unit]()
    val listener = new MyListener(failBatches = Set.empty)({
      me =>
        if (me.sinkedPoints.size == 2 && me.droppedPoints.size == 2) {
          val isFail =
            me.sinkedPoints.exists(_.exists(_.getMetric != "send")) ||
              me.droppedPoints.exists(_.exists(_.getMetric != "drop"))
          if (isFail) {
            testCompletion.failure(new AssertionError("strategy failed"))
          } else {
            testCompletion.success(())
          }
        }
    })
    val dropStrategy: DropStrategy = {
      points =>
        val (sendPoints, dropPoints) = points.partition(_.getMetric == "send")
        SendAndDrop(PackedPoints(sendPoints), PackedPoints(dropPoints))
    }
    createConsumer(config, source, listener, dropStrategy)
    Await.result(testCompletion.future, 5 seconds)
  }

  it should "send points in parallel" in {
    val numberOfBatches: Int = 20
    val config: Config = mkConfig(maxParallelSenders = numberOfBatches, tick = 1)
    val points = Seq(createPoint(), createPoint())
    val batches = (1 to numberOfBatches).map(i => i.toLong -> points)
    val source = createPointsSource(batches: _*)
    val testCompletion = Promise[Unit]()
    val batchesCount = new AtomicInteger(0)
    source.commitOffsets() answers {
      mock =>
        if (batchesCount.get == numberOfBatches) {
          testCompletion.success(())
        }
    }
    val sendAllStrategy: DropStrategy = points => SendAndDrop(pointsToSend = points)
    val listener = new MyListener(failBatches = Set.empty, parallelism = numberOfBatches)({
      me =>
        batchesCount.incrementAndGet()
        Thread.sleep(200)
    })
    createConsumer(config, source, listener, sendAllStrategy)
    Await.result(testCompletion.future, 500 millis)
  }

  it should "commit offsets at least every 'max-parallel-senders' batches" in {
    val numberOfBatches: Int = 20
    val maxParallelSenders = 5
    val config = mkConfig(maxParallelSenders, tick = 1)
    val points = Seq(createPoint(), createPoint())
    val batches = (1 to numberOfBatches).map(i => i.toLong -> points)
    val source = createPointsSource(batches: _*)
    val testCompletion = Promise[Unit]()
    val batchesCount = new AtomicInteger(0)
    val batchesFromLastCommit = new AtomicInteger(0)
    source.commitOffsets() answers {
      mock =>
        if (batchesFromLastCommit.get() > maxParallelSenders) {
          testCompletion.failure(new AssertionError())
        }
        if (batchesCount.get == numberOfBatches) {
          testCompletion.success(())
        }
        batchesFromLastCommit.set(0)
    }
    val sendAllStrategy: DropStrategy = points => SendAndDrop(pointsToSend = points)
    val listener = new MyListener(failBatches = Set.empty, parallelism = 1)({
      me =>
        batchesCount.incrementAndGet()
        batchesFromLastCommit.incrementAndGet()
        Thread.sleep(10)
    })
    createConsumer(config, source, listener, sendAllStrategy)
    Await.result(testCompletion.future, 500 millis)
  }

  def createConsumer(config: Config,
                     source: PointsSource,
                     listener: MyListener,
                     dropStrategy: DropStrategy) = {
    val dconf = KafkaPointsConsumerConfig("test", "test", config)
    val metrics = new KafkaPointsConsumerMetrics("test", new MetricRegistry)
    TestActorRef(Props.apply(new KafkaPointsConsumer(
      dconf,
      metrics,
      source,
      listener.sinkPipe,
      listener.dropPipe,
      dropStrategy,
      idGenerator,
      ectx
    )))
  }

  def createPointsSource(messageSeqs: (Long, Seq[Message.Point])*) = {
    val source = mock[PointsSource]
    val (firstBatchId, firstMessageSeq) = messageSeqs.head
    val restSeqs = messageSeqs.tail
    val ongoingStubbing = source.consume() answers {
      mock => Some(firstBatchId -> PackedPoints.apply(firstMessageSeq))
    }
    restSeqs.foldLeft(ongoingStubbing) {
      case (stubbing, (batchId, messageSeq)) =>
        stubbing thenAnswers {
          mock => Some(batchId -> PackedPoints.apply(messageSeq))
        }
    } thenAnswers {
      mock => None
    }
    source
  }
}

class MyListener(failBatches: Set[Long], parallelism: Int = 1)(callback: (MyListener) => Unit) {
  val sinkedBatches = new AtomicList[Long]
  val droppedBatches = new AtomicList[Long]
  val sinkedPoints = new AtomicList[PackedPoints]
  val droppedPoints = new AtomicList[PackedPoints]
  implicit val exct = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))

  def sinkPipe: PointsSink = new BatchCounter(sinkedBatches, sinkedPoints)

  def dropPipe: PointsSink = new BatchCounter(droppedBatches, droppedPoints, checkFail = false)

  class BatchCounter(val batches: AtomicList[Long], val points: AtomicList[PackedPoints],
                     checkFail: Boolean = true) extends PointsSink {
    override def sendPoints(batchId: Long, points: Point*): Future[Long] = {
      registerBatch(batchId, PackedPoints(points))
    }

    override def sendPacked(batchId: Long, buffers: PackedPoints*): Future[Long] = {
      registerBatch(batchId, buffers: _*)
    }

    def registerBatch(batchId: Long, buffers: PackedPoints*): Future[Long] = {
      failBatches.find(_ == batchId) match {
        case Some(x) if checkFail =>
          Future.failed(new IllegalArgumentException("Expected exception"))
        case None =>
          var count = 0l;
          batches.add(batchId)
          for (buf <- buffers) {
            points.add(buf)
            count += buf.pointsCount
          }
          Future {
            callback.apply(MyListener.this)
            count
          }
      }
    }

    override def close() = {}
  }
}
