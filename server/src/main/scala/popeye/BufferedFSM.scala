package popeye

import scala.collection.immutable
import akka.actor.FSM
import scala.concurrent.duration.FiniteDuration
import nl.grons.metrics.scala.Gauge

object BufferedFSM {

  sealed trait State

  case object Idle extends State

  case object Active extends State

  case object Stop extends State

  sealed case class Todo[Entity](entityCnt: Long = 0, queue: immutable.Seq[Entity] = Vector.empty)

  case object Flush

  case object FlushStop
}

import BufferedFSM._

trait BufferedFSM[Entity] extends FSM[State, Todo[Entity]] {

  type TodoFunction = PartialFunction[Event, Todo[Entity]]

  def timeout: FiniteDuration
  def flushEntitiesCount: Int

  def consumeCollected(data: Todo[Entity]): Unit

  val handleMessage: TodoFunction

  startWith(Idle, Todo())

  when(Stop) {
    case _ =>
      context.stop(self)
      consumeCollected(stateData)
      stay
  }

  when(Idle) {
    case Event(Flush, _) =>
      stay // nothing to do
    case Event(FlushStop, _) =>
      context.stop(self)
      stay
  }

  when(Active, stateTimeout = timeout) {
    case Event(Flush | StateTimeout, _) =>
      goto(Idle) using Todo()
    case Event(FlushStop, _) =>
      goto(Stop) using Todo()
  }


  onTransition {
    case Active -> Idle | Active -> Stop ⇒
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
        self ! Flush
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

