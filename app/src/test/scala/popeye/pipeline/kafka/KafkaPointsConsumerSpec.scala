package popeye.pipeline.kafka

import akka.actor.Props
import akka.testkit.TestActorRef
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import java.util.Random
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import popeye.pipeline.{PointsSink, PointsSource, AtomicList}
import popeye.test.{AkkaTestKitSpec, PopeyeTestUtils, MockitoStubs}
import popeye.test.PopeyeTestUtils._
import popeye.proto.{Message, PackedPoints}
import scala.concurrent.{ExecutionContext, Promise, Await}
import scala.concurrent.duration._
import popeye.pipeline.kafka.KafkaPointsConsumer.DropStrategy
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import java.util.concurrent.Executors
import scala.concurrent.Future
import popeye.proto.Message.Point
import popeye.{Logging, IdGenerator}
import org.mockito.Matchers._


/**
 * @author Andrey Stepachev
 */
class KafkaPointsConsumerSpec extends AkkaTestKitSpec("KafkaPointsConsumer")
with MockitoSugar with MockitoStubs with ShouldMatchers with Logging {

  implicit val sch = system.scheduler
  implicit val ectx = system.dispatcher
  implicit val rnd = new Random(1234)
  val idGenerator = new IdGenerator(1)

  "Dispatcher" should "buffer" in {
    val config = mkConfig()
    val events1 = 1l -> PopeyeTestUtils.mkEvents(3)
    val events2 = 2l -> PopeyeTestUtils.mkEvents(3)
    val source = createPointsSource(events1, events2)
    val testCompletion = Promise[Unit]()
    val listener = new MyListener(failBatches = Set(2))({
      me =>
        debug(s"sinkedBatches=${me.sinkedBatches}, droppedBatches=${me.droppedBatches}")
      if (me.sinkedBatches.size == 1 && me.droppedBatches.size == 1) testCompletion.success(())
    })
    val noOpStrategy: PackedPoints => SendAndDrop = points => SendAndDrop(pointsToSend = points)
    createConsumer(config, source, listener.mainSink, listener.dropSink, noOpStrategy)
    Await.result(testCompletion.future, 5 seconds)
  }

  def mkConfig(tick: FiniteDuration = 500 millis,
               backoff: FiniteDuration = 1 second,
               batchSize: Int = 2,
               maxParallelSenders: Int = 1): KafkaPointsConsumerConfig =
    KafkaPointsConsumerConfig(tick, backoff, batchSize, maxParallelSenders)

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
    createConsumer(config, source, listener.mainSink, listener.dropSink, dropStrategy)
    Await.result(testCompletion.future, 5 seconds)
  }

  it should "send points in parallel" in {
    val numberOfBatches: Int = 20
    val config = mkConfig(tick = 1 milli, maxParallelSenders = numberOfBatches)
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
    createConsumer(config, source, listener.mainSink, listener.dropSink, sendAllStrategy)
    Await.result(testCompletion.future, 500 millis)
  }

  it should "commit offsets at least every 'max-parallel-senders' batches" in {
    val numberOfBatches: Int = 20
    val maxParallelSenders = 5
    val config = mkConfig(tick = 1 milli, maxParallelSenders = maxParallelSenders)
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
    createConsumer(config, source, listener.mainSink, listener.dropSink, sendAllStrategy)
    Await.result(testCompletion.future, 500 millis)
  }

  it should "try to redeliver a failed batch" in {
    val config = mkConfig(maxParallelSenders = 1, tick = 10 millis, backoff = 500 millis)
    val points = Seq(createPoint(), createPoint())
    val source = createPointsSource(0l -> points)
    val sendAllStrategy: DropStrategy = points => SendAndDrop(pointsToSend = points)
    val unreliableSink = mock[PointsSink]
    val testCompletion = Promise[Unit]()
    val failTime = new AtomicLong()
    unreliableSink.sendPacked(any[Long], any[PackedPoints]) answers {
      _ => Future.failed(new RuntimeException)
    } thenAnswers {
      _ =>
        if (failTime.get < System.currentTimeMillis() - 200) {
          testCompletion.success(())
        } else {
          testCompletion.failure(new AssertionError("backoff time is too short"))
        }
        Future.successful(1l)
    }
    val failSink = mock[PointsSink]
    failSink.sendPacked(any[Long], any[Seq[PackedPoints]]: _*) answers {
      _ =>
        failTime.set(System.currentTimeMillis())
        Future.failed(new RuntimeException)
    }
    createConsumer(
      config = config,
      source = source,
      mainSink = unreliableSink,
      dropSink = failSink,
      dropStrategy = sendAllStrategy
    )
    Await.result(testCompletion.future, 5 second)
  }

  def createConsumer(config: KafkaPointsConsumerConfig,
                     source: PointsSource,
                     mainSink: PointsSink,
                     dropSink: PointsSink,
                     dropStrategy: DropStrategy) = {
    val metrics = new KafkaPointsConsumerMetrics("test", new MetricRegistry)
    TestActorRef(Props.apply(new KafkaPointsConsumer(
      config,
      metrics,
      source,
      mainSink,
      dropSink,
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

class MyListener(failBatches: Set[Long], parallelism: Int = 1)(callback: (MyListener) => Unit) extends Logging{
  val sinkedBatches = new AtomicList[Long]
  val droppedBatches = new AtomicList[Long]
  val sinkedPoints = new AtomicList[PackedPoints]
  val droppedPoints = new AtomicList[PackedPoints]
  implicit val exct = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))

  def mainSink: PointsSink = new BatchCounter(sinkedBatches, sinkedPoints)

  def dropSink: PointsSink = new BatchCounter(droppedBatches, droppedPoints, checkFail = false)

  class BatchCounter(val batches: AtomicList[Long], val points: AtomicList[PackedPoints],
                     checkFail: Boolean = true) extends PointsSink {
    override def sendPoints(batchId: Long, points: Point*): Future[Long] = {
      registerBatch(batchId, PackedPoints(points))
    }

    override def sendPacked(batchId: Long, buffers: PackedPoints*): Future[Long] = {
      registerBatch(batchId, buffers: _*)
    }

    def deliver(batchId: Long, buffers: PackedPoints*): Future[Long] = {
      debug(s"Accounted batch $batchId")
      var count = 0l
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

    def registerBatch(batchId: Long, buffers: PackedPoints*): Future[Long] = {
      debug(s"Got batch $batchId")
      if (checkFail) {
        failBatches.find(_ == batchId) match {
          case Some(x) =>
            debug(s"Failed batch $batchId")
            Future.failed(new IllegalArgumentException("Expected exception"))
          case None =>
            deliver(batchId, buffers :_*)
        }
      } else {
        deliver(batchId, buffers :_*)
      }
    }

    override def close() = {}
  }
}
