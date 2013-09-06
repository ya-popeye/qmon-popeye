package popeye.storage.hbase

import akka.actor.Deploy
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, Config}
import java.util.Random
import java.util.concurrent.CountDownLatch
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import popeye.test.{MockitoStubs, PopeyeTestUtils}
import popeye.test.PopeyeTestUtils.MockPopeyeConsumerFacotory
import popeye.transport.test.AkkaTestKitSpec
import popeye.{ConfigUtil, Logging}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import popeye.transport.proto.Message

/**
 * @author Andrey Stepachev
 */
class HBasePointConsumerSpec extends AkkaTestKitSpec("uniqueid") with MockitoStubs with Logging {

  implicit val rnd = new Random(1234)
  implicit val timeout: Timeout = 5 seconds
  implicit val metricRegistry = new MetricRegistry()
  implicit val hbaseConsumerMetrics = new HBasePointConsumerMetrics(metricRegistry)

  private def initActors() = {
    val config: Config = ConfigFactory.parseString(
      s"""
        | zk.cluster = "localhost:2181"
      """.stripMargin)
      .withFallback(ConfigUtil.loadSubsysConfig("pump"))
      .resolve()

    val storage = mock[PointsStorage]

    val consumer = new MockPopeyeConsumerFacotory
    val actor: TestActorRef[HBasePointConsumer] = TestActorRef(
      HBasePointConsumer.props(config, storage, consumer)
        .withDeploy(Deploy.local))
    (actor, storage, consumer)
  }

  behavior of "HBasePointConsumer"

  it should "read as expected" in {
    val (actor, storage, consumer) = initActors()
    consumer.consumer.addMessages(1, PopeyeTestUtils.mkEvents(2))
    val lock = new CountDownLatch(1)
    within (3 seconds) {
      storage.writePoints(any())(any()) returns {
          lock.countDown()
          Promise[Int]().success(1).future
      }
      lock.await()
    }

  }

}
