package popeye.transport.kafka

import akka.actor.{Actor, Props}
import akka.testkit.TestActorRef
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigValueFactory, ConfigFactory, Config}
import java.util.Random
import java.util.concurrent.CountDownLatch
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import popeye.pipeline.{PointsSource, PointsSink, AtomicList}
import popeye.test.{PopeyeTestUtils, MockitoStubs}
import popeye.transport.proto.{PackedPoints}
import popeye.transport.test.AkkaTestKitSpec
import scala.concurrent.Future


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
    val dconf = new KafkaPointsConsumerConfig(mkConfig().withValue("tick", ConfigValueFactory.fromAnyRef(10000)))
    val consumer = mock[PointsSource]
    val events1: consumer.BatchedMessageSet = 1l -> PopeyeTestUtils.mkEvents(3)
    val events2: consumer.BatchedMessageSet = 2l -> PopeyeTestUtils.mkEvents(3)
    consumer.consume() answers {
      mock => Some(events1)
    } thenAnswers {
      mock =>
        Some(events2)
    } thenAnswers {
      mock => None
    }
    val latch = new CountDownLatch(1)
    val listener = new MyListener(Set(2), { me =>
      if (me.sinkedBatches.size == 1 && me.droppedBatches.size == 1) latch.countDown()
    })
    val actor: TestActorRef[KafkaPointsConsumer] = TestActorRef(
      Props.apply(new KafkaPointsConsumer(dconf, metrics, consumer, listener.sinkPipe, listener.dropPipe)))
    latch.await()
  }

  def mkConfig(): Config = ConfigFactory.parseString(
    s"""
    | topic = popeye-points
    | group = test
    | batch-size = 2
    | max-lag = 60s
      """.stripMargin)
    .withFallback(ConfigFactory.parseResources("reference.conf"))
    .resolve()

}

class MyListener(val failBatches: Set[Long],
                 val callback: (MyListener) => Unit) {
  val sinkedBatches = new AtomicList[Long]
  val droppedBatches = new AtomicList[Long]

  def sinkPipe: PointsSink = new PointsSink {
    def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
      batchIds.find(failBatches.contains) match {
        case Some(x) =>
          Future.failed(new IllegalArgumentException("Expected exception"))
        case None =>
          batchIds.foreach {
            sinkedBatches.add
          }
          callback.apply(MyListener.this)
          Future.successful(batchIds.length)
      }
    }

    def close() = {}
  }

  def dropPipe: PointsSink = new PointsSink {
    def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
      batchIds.foreach {
        droppedBatches.add
      }
      callback.apply(MyListener.this)
      Future.successful(1)
    }

    def close() = {}
  }
}
