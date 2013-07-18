package popeye

import scala.collection.immutable
import akka.actor.FSM
import scala.concurrent.duration.FiniteDuration

object BufferedFSM {

  sealed trait State

  case object Idle extends State

  case object Active extends State

  sealed case class Todo[Entity](entityCnt: Long = 0, queue: immutable.Seq[Entity] = Vector.empty)

  sealed case class Flush()
}

import BufferedFSM._

trait BufferedFSM[Entity] extends FSM[State, Todo[Entity]] {

  type TodoFunction = PartialFunction[Event, Todo[Entity]]

  val timeout: FiniteDuration
  val flushEntitiesCount: Int

  def consumeCollected(data: Todo[Entity]): Unit

  val handleMessage: TodoFunction

  startWith(Idle, Todo())

  when(Idle) {
    case Event(Flush(), _) =>
      stay // nothing to do
  }

  when(Active, stateTimeout = timeout) {
    case Event(x: Flush, _) =>
      goto(Idle) using Todo()

    case Event(StateTimeout, _) =>
      goto(Idle) using Todo()
  }


  onTransition {
    case Active -> Idle ⇒
      stateData match {
        case t@Todo(eventsCnt, queue) =>
          if (log.isDebugEnabled)
            log.debug("Flushing queue {} ({} events)", queue.size, eventsCnt)
          consumeCollected(t)
      }
  }

  whenUnhandled {
    case e @ Event(message, t: Todo[Entity]) =>
      if (t.entityCnt >= flushEntitiesCount)
        self ! Flush()
      val rv = (handleMessage orElse unhandledEvent)(e)
      rv match {
        case Nil =>
          stay
        case t: Todo[Entity] =>
          goto(Active) using t
      }
  }

  onTermination {
    case StopEvent(cause, _, todo) =>
      log.debug("Terminated " + cause)
      consumeCollected(todo)
  }

  val unhandledEvent: PartialFunction[Any, Any] = {
    case Event(e, s) ⇒
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      List()
  }

}

