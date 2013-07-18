package popeye

import popeye.BufferedFSM.{Flush, Todo}
import scala.concurrent.duration._
import akka.testkit.TestActorRef
import akka.actor.Props
import akka.pattern.ask
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import popeye.transport.test.AkkaTestKitSpec
import akka.util.Timeout
import org.scalatest._


/**
 * @author Andrey Stepachev
 */
class BufferedFSMSpec extends AkkaTestKitSpec("ProducerTest") with Logging {

  implicit val timeout: Timeout = 2 seconds

  "BufferFST" should "work" in {
    val fsm = TestActorRef(Props(new TestFSM))
    fsm ! "hello"
    fsm ! "me"
    expect(2) {
      Await.result(fsm.ask("how-much").mapTo[Long], 1 seconds)
    }
    fsm ! Flush()
    expect(0) {
      Await.result(
      fsm.ask("how-much").mapTo[Long], 1 seconds)
    }
    fsm ! "hello"
    fsm ! "me"
    fsm ! "hello"
    fsm ! "me"
    expect(1) {
      Await.result(
        fsm.ask("how-much").mapTo[Long], 1 seconds)
    }
  }
}

class TestFSM extends BufferedFSM[String] {
  val timeout: FiniteDuration = 999 second
  val flushEntitiesCount: Int = 2

  def events = new AtomicInteger(0)

  def consumeCollected(data: Todo[String]) {
    events.addAndGet(data.queue.size)
  }

  override val handleMessage: TodoFunction = {
    case Event("how-much", todo) =>
      sender ! todo.entityCnt
      todo
    case Event(s: String, todo) =>
      todo.copy(entityCnt = todo.entityCnt + 1, queue = todo.queue :+ s)
  }
}
