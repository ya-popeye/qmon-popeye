package popeye.storage.hbase

import akka.actor._
import akka.actor.SupervisorStrategy.Restart
import popeye.storage.hbase.BytesKey._
import popeye.storage.hbase.UniqueIdActor.RequestsServed
import popeye.storage.hbase.UniqueIdProtocol._

import HBaseStorage._

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object UniqueIdProtocol {

  sealed trait Request

  case class FindName(qname: QualifiedName, create: Boolean = true) extends Request

  case class FindId(qid: QualifiedId) extends Request

  sealed trait Response

  case class Resolved(name: ResolvedName) extends Response

  case class Race(name: QualifiedName) extends Response

  case class ResolutionFailed(t: Throwable) extends Response

  sealed trait NotFound extends Response

  case class NotFoundName(qname: QualifiedName) extends NotFound

  case class NotFoundId(qid: QualifiedId) extends NotFound

}

trait UniqueIdStorageTrait {
  def findByName(qnames: Seq[QualifiedName]): Seq[ResolvedName]


  def findById(ids: Seq[QualifiedId]): Seq[ResolvedName]

  def registerName(qname: QualifiedName): ResolvedName
}

object UniqueIdActor {

  case class StateData(requestQueue: Queue[(ActorRef, Request)])

  sealed trait State

  case object Idle extends State

  case object Resolving extends State

  case object RequestsServed

  def apply(storage: UniqueIdStorage, executionContext: ExecutionContext, batchSize: Int = 100) = {
    val storageWrapper = new UniqueIdStorageTrait {
      def findByName(qnames: Seq[QualifiedName]): Seq[ResolvedName] = storage.findByName(qnames)

      def findById(ids: Seq[QualifiedId]): Seq[ResolvedName] = storage.findById(ids)

      def registerName(qname: QualifiedName): ResolvedName = storage.registerName(qname)
    }
    new UniqueIdActor(storageWrapper, executionContext, batchSize)
  }
}

/**
 * @author Andrey Stepachev
 */

class UniqueIdActor(storage: UniqueIdStorageTrait, executionContext: ExecutionContext, batchSize: Int = 1000)
  extends FSM[UniqueIdActor.State, UniqueIdActor.StateData] {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ => Restart
  }

  implicit val exct = executionContext

  startWith(UniqueIdActor.Idle, UniqueIdActor.StateData(Queue.empty))

  when(UniqueIdActor.Idle) {
    case Event(r: Request, _) =>
      resolveNameAndIds(Seq((sender, r))).onComplete(_ => self ! RequestsServed)
      goto(UniqueIdActor.Resolving) using UniqueIdActor.StateData(Queue.empty)
  }

  when(UniqueIdActor.Resolving) {
    case Event(r: Request, state) =>
      val queueWithNewRequest = state.requestQueue.enqueue((sender, r))
      val onlyFreshRequests = queueWithNewRequest.drop(queueWithNewRequest.size - batchSize)
      stay() using state.copy(requestQueue = onlyFreshRequests)

    case Event(RequestsServed, state) =>
      if (state.requestQueue.nonEmpty) {
        resolveNameAndIds(state.requestQueue).onComplete(_ => self ! RequestsServed)
        stay() using state.copy(requestQueue = Queue.empty)
      } else {
        goto(UniqueIdActor.Idle)
      }
  }

  def resolveNameAndIds(requests: Seq[(ActorRef, Request)]): Future[Unit] = {
    var idRequests = Map[QualifiedId, List[ActorRef]]()
    var lookupRequests = Map[QualifiedName, List[ActorRef]]()
    var createRequests = Map[QualifiedName, List[ActorRef]]()
    for ((sender, request) <- requests) {
      request match {
        case r: FindId =>
          val list = idRequests.getOrElse(r.qid, List.empty)
          idRequests = idRequests.updated(r.qid, list ++ List(sender))

        case r: FindName =>
          @inline
          def addTo(map: Map[QualifiedName, List[ActorRef]]): Map[QualifiedName, List[ActorRef]] = {
            val list = map.getOrElse(r.qname, List.empty)
            map.updated(r.qname, list ++ List(sender))
          }
          if (r.create) {
            createRequests = addTo(createRequests)
          } else {
            lookupRequests = addTo(lookupRequests)
          }
      }
    }
    Future {
      if (lookupRequests.nonEmpty || createRequests.nonEmpty) {
        checkNames(lookupRequests, createRequests)
      }
      if (idRequests.nonEmpty) {
        checkIds(idRequests)
      }
    }
  }

  private def checkIds(idRequests: Map[QualifiedId, List[ActorRef]]) = {
    val attempt = Try(storage.findById(idRequests.keys.toSeq).map(rName => (rName.toQualifiedId, rName)).toMap)
    attempt match {
      case Success(resolvedNames) => sendResolvedIdMessages(resolvedNames, idRequests)
      case Failure(t) => idRequests.values.flatten.foreach(_ ! ResolutionFailed(t))
    }
  }

  private def sendResolvedIdMessages(resolvedNames: Map[QualifiedId, ResolvedName],
                                     idRequests: Map[QualifiedId, List[ActorRef]]) = {
    for ((qId, actorRefs) <- idRequests) {
      val message = resolvedNames.get(qId) match {
        case Some(resolvedName) => Resolved(resolvedName)
        case None => NotFoundId(qId)
      }
      actorRefs.foreach(actor => actor ! message)
    }
  }

  private def checkNames(lookupRequests: Map[QualifiedName, List[ActorRef]],
                         createRequests: Map[QualifiedName, List[ActorRef]]) = {
    val keys = lookupRequests.keySet ++ createRequests.keySet
    log.info(s"lookup for $keys")
    val attempt = Try(storage.findByName(keys.toSeq).map(resolved => (resolved.toQualifiedName, resolved)).toMap)
    log.info(s"lookup result: $attempt")
    attempt match {
      case Success(resolvedNames) => sendResolvedNamesMessages(resolvedNames, lookupRequests, createRequests)
      case Failure(t) => (lookupRequests.values ++ createRequests.values).flatten.foreach(_ ! ResolutionFailed(t))
    }
  }

  private def sendResolvedNamesMessages(resolvedNames: Map[QualifiedName, ResolvedName],
                                        lookupRequests: Map[QualifiedName, List[ActorRef]],
                                        createRequests: Map[QualifiedName, List[ActorRef]]) = {
    for ((qName, actorRefs) <- lookupRequests) {
      val message = resolvedNames.get(qName) match {
        case Some(rName) => Resolved(rName)
        case None => NotFoundName(qName)
      }
      actorRefs.foreach(actor => actor ! message)
    }
    for ((qName, actorRefs) <- createRequests) {
      val message = resolvedNames.get(qName) match {
        case Some(rname) => Resolved(rname)
        case None =>
          try {
            val result = storage.registerName(qName)
            Resolved(result)
          } catch {
            case ex: UniqueIdRaceException =>
              log.error(ex, "Race")
              Race(qName)
            case ex: Throwable =>
              log.error(ex, "UniqueIdActor got error")
              ResolutionFailed(ex)
          }
      }
      actorRefs.foreach(actor => actor ! message)
    }
  }
}