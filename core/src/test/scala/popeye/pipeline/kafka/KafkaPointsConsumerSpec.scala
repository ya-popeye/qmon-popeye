package popeye.pipeline.kafka

import akka.actor.Props
import akka.testkit.TestActorRef
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import java.util.Random
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import popeye.pipeline.{PointsSource, PointsSink, AtomicList}
import popeye.test.{PopeyeTestUtils, MockitoStubs}
import popeye.proto.{Message, PackedPoints}
import popeye.pipeline.test.AkkaTestKitSpec
import scala.concurrent.{Promise, Await, Future}
import scala.concurrent.duration._
import popeye.pipeline.kafka.KafkaPointsConsumer.DropStrategy
import popeye.proto.Message.Point


/**
 * @author Andrey Stepachev
 */
class KafkaPointsConsumerSpec extends AkkaTestKitSpec("KafkaPointsConsumer") with MockitoSugar with MockitoStubs with ShouldMatchers {

  val registry = new MetricRegistry
  implicit val sch = system.scheduler
  implicit val ectx = system.dispatcher
  implicit val rnd = new Random(1234)

  "Dispatcher" should "buffer" in {
    val metrics = new KafkaPointsConsumerMetrics("test", registry)
    val config = mkConfig()//.withValue("tick", ConfigValueFactory.fromAnyRef(10000))
    val dconf = new KafkaPointsConsumerConfig("test", "test", config)
    val events1 = 1l -> PopeyeTestUtils.mkEvents(3)
    val events2 = 2l -> PopeyeTestUtils.mkEvents(3)
    val source = createPointsSource(events1, events2)
    val testСompletion = Promise[Unit]()
    val listener = new MyListener(Set(2), { me =>
      if (me.sinkedBatches.size == 1 && me.droppedBatches.size == 1) testСompletion.success(())
    })
    val noOpStrategy: Seq[Point] => SendAndDrop = points => SendAndDrop(pointsToSend = points)
    val actor: TestActorRef[KafkaPointsConsumer] = TestActorRef(
      Props.apply(new KafkaPointsConsumer(dconf, metrics, source, listener.sinkPipe, listener.dropPipe, noOpStrategy, ectx)))
    Await.result(testСompletion.future, 5 seconds)
  }

  def mkConfig(): Config = ConfigFactory.parseString(
    s"""
    | topic = popeye-points
    | group = test
    | batch-size = 2
    | max-lag = 60s
      """.stripMargin)
    .withFallback(ConfigFactory.parseResources("reference.conf").getConfig("common.popeye.pipeline.kafka.consumer"))
    .resolve()

  it should "use drop strategy" in {
    val metrics = new KafkaPointsConsumerMetrics("test", registry)
    val config = mkConfig()
    val dconf = new KafkaPointsConsumerConfig("test", "test", config)
    val points = Seq(PopeyeTestUtils.createPoint(metric = "send"), PopeyeTestUtils.createPoint(metric = "drop"))
    val events1 = 1l -> points
    val events2 = 2l -> points
    val source = createPointsSource(events1, events2)
    val testСompletion = Promise[Unit]()
    val listener = new MyListener(failBatches = Set.empty, {
      me =>
        if (me.sinkedPoints.size == 2 && me.sinkedPoints.size == 2) {
          val isFail =
            me.sinkedPoints.exists(_.exists(_.getMetric != "send")) ||
              me.droppedPoints.exists(_.exists(_.getMetric != "drop"))
          if (isFail) {
            testСompletion.failure(new AssertionError("strategy failed"))
          } else {
            testСompletion.success(())
          }
        }
    })
    val dropBigBatch: DropStrategy = {
      points =>
        val (sendPoints, dropPoints) = points.partition(_.getMetric == "send")
        SendAndDrop(sendPoints, dropPoints)
    }
    val actor: TestActorRef[KafkaPointsConsumer] = TestActorRef(
      Props.apply(new KafkaPointsConsumer(dconf, metrics, source, listener.sinkPipe, listener.dropPipe, dropBigBatch, ectx)))
    Await.result(testСompletion.future, 5 seconds)
  }


  def createPointsSource(messageSeqs: (Long, Seq[Message.Point])*) = {
    val source = mock[PointsSource]
    val firstMessageSeq = messageSeqs.head
    val restSeqs = messageSeqs.tail
    val ongoingStubbing = source.consume() answers {
      mock => Some(firstMessageSeq)
    }
    restSeqs.foldLeft(ongoingStubbing) {
      case (stubbing, messageSeq) =>
        stubbing thenAnswers {
          mock => Some(messageSeq)
        }
    } thenAnswers {
      mock => None
    }
    source
  }
}

class MyListener(val failBatches: Set[Long],
                 val callback: (MyListener) => Unit) {
  val sinkedBatches = new AtomicList[Long]
  val droppedBatches = new AtomicList[Long]
  val sinkedPoints = new AtomicList[PackedPoints]
  val droppedPoints = new AtomicList[PackedPoints]

  def sinkPipe: PointsSink = new PointsSink {
    def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
      batchIds.find(failBatches.contains) match {
        case Some(x) =>
          Future.failed(new IllegalArgumentException("Expected exception"))
        case None =>
          batchIds.foreach {
            sinkedBatches.add
          }
          sinkedPoints.add(points)
          callback.apply(MyListener.this)
          Future.successful(batchIds.length)
      }
    }
  }

  def dropPipe: PointsSink = new PointsSink {
    def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
      batchIds.foreach {
        droppedBatches.add
      }
      droppedPoints.add(points)
      callback.apply(MyListener.this)
      Future.successful(1)
    }
  }
}
