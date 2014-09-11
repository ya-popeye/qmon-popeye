package popeye.util

import java.util.concurrent.Executors

import org.scalatest.{Matchers, FlatSpec}
import popeye.pipeline.test.AkkaTestKitSpec

import scala.concurrent.{Promise, Await, Future, ExecutionContext}
import scala.concurrent.duration._

class FutureStreamSpec extends AkkaTestKitSpec("test") {
  implicit val exct = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  behavior of "FutureStream.withTimeout"

  it should "behave like normal stream if timeout was not invoked" in {
    val stream = FutureStream.fromItems(1, 2, 3, 4)
    val streamWithTimeout = stream.withCancellation(Promise().future)
    Await.result(streamWithTimeout.reduceElements(_ + _), 1 second) should equal(10)
  }

  it should "cancel execution if head future is not ready" in {
    val stream = FutureStream(delayed(10 second)(1), () => Future.successful(None))
    assertTimeout(stream, 10 millis)
  }

  it should "cancel execution if tail future is not ready" in {
    val stream = FutureStream(Future.successful(0), () => delayed(10 second)(None))
    assertTimeout(stream, 10 millis)
  }

  it should "cancel execution if next stream is not ready" in {
    val nextStream = FutureStream(delayed(10 second)(1), () => Future.successful(None))
    val stream = FutureStream(Future.successful(0), () => Future.successful(Some(nextStream)))
    assertTimeout(stream, 10 millis)
  }

  def delayed[A](duration: FiniteDuration)(block: => A) = Future {
    Thread.sleep(duration.toMillis)
    block
  }

  def assertTimeout(stream: FutureStream[Int], timeout: FiniteDuration) {
    val cancellationPromise = Promise[Nothing]()
    system.scheduler.scheduleOnce(timeout) {
      cancellationPromise.failure(new RuntimeException("timeout"))
    }
    val streamWithTimeout = stream.withCancellation(cancellationPromise.future)
    val ex = intercept[RuntimeException] {
      Await.result(streamWithTimeout.reduceElements(_ + _), 1 second)
    }
    ex.getMessage should equal("timeout")
  }


}
