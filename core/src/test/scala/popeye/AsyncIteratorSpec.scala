package popeye

import java.util.concurrent.Executors

import akka.dispatch.ExecutionContexts
import org.scalatest.{Matchers, FlatSpec}
import AsyncIterator._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class AsyncIteratorSpec extends FlatSpec with Matchers {

  implicit val exct = ExecutionContexts.fromExecutor(Executors.newSingleThreadExecutor())

  behavior of "AsyncIterator.iterate"

  it should "create a singleton iterator" in {
    val iter = iterate(Future.successful(0)) {
      i => None
    }
    toSeq(iter) should equal(Seq(0))
  }

  it should "create a simple iterator" in {
    val iter = iterate(Future.successful(0)) {
      i =>
        if (i < 5) {
          Some(Future.successful(i + 1))
        } else {
          None
        }
    }
    toSeq(iter) should equal(Seq(0, 1, 2, 3, 4, 5))
  }

  def toSeq[A](iter: AsyncIterator[A]) = {
    Await.result(toStrictSeq(iter), Duration.Inf)
  }
}
