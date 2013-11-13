package popeye.storage.hbase

import akka.actor._
import popeye.Logging
import popeye.storage.hbase.BytesKey._
import popeye.storage.hbase.UniqueIdProtocol.NotFoundName
import popeye.storage.hbase.UniqueIdProtocol.FindName
import popeye.storage.hbase.UniqueIdProtocol.NotFoundId
import popeye.storage.hbase.UniqueIdProtocol.Resolved
import scala.Some
import popeye.storage.hbase.UniqueIdProtocol.FindId
import popeye.storage.hbase.UniqueIdProtocol.Race
import akka.actor.Terminated
import popeye.storage.hbase.UniqueIdProtocol.ResolutionFailed
import akka.actor.SupervisorStrategy.Restart

import HBaseStorage._

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


/**
 * @author Andrey Stepachev
 */
class UniqueIdActor(storage: UniqueIdStorage,
                    batchSize: Int = 100)
  extends Actor with Logging {


  private var idRequests = Map[QualifiedId, List[ActorRef]]()
  private var lookupRequests = Map[QualifiedName, List[ActorRef]]()
  private var createRequests = Map[QualifiedName, List[ActorRef]]()

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ => Restart
  }

  override def postStop() {
    idRequests.foreach {entry => sendResolutionFailed(entry._2)}
    lookupRequests.foreach {entry => sendResolutionFailed(entry._2)}
    createRequests.foreach {entry => sendResolutionFailed(entry._2)}
    super.postStop()
  }

  def sendResolutionFailed(actors: Seq[ActorRef]) {
    actors.map{ar => ar ! ResolutionFailed(new InterruptedException("Stopped Actor"))}
  }

  def receive: Actor.Receive = {

    case Terminated(watched) =>
      context unwatch watched // Do we really need this?
      idRequests = idRequests.transform { (k, v) => v.filter(_ != watched)}
      lookupRequests = lookupRequests.transform { (k, v) => v.filter(_ != watched)}
      createRequests = createRequests.transform { (k, v) => v.filter(_ != watched)}

    case r: FindId =>
      val list = idRequests.getOrElse(r.qid, List.empty)
      idRequests = idRequests.updated(r.qid, list ++ List(sender))
      context watch sender
      checkIds()

    case r: FindName =>
      context watch sender
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
      checkNames()
  }

  private def checkIds() = {
    try {
      val resolved = storage.findById(idRequests.keys.toSeq)
        .map(rname => (new BytesKey(rname.id), rname))
        .toMap
      idRequests.foreach { tuple =>
        resolved.get(tuple._1.id) match {
          case Some(name) =>
            tuple._2.foreach { ref => ref ! Resolved(name)}
          case None =>
            tuple._2.foreach { ref => ref ! NotFoundId(tuple._1)}
        }
      }
    } finally {
      idRequests = Map()
    }
  }

  private def checkNames() = {
    var recreate = Map[QualifiedName, List[ActorRef]]()
    try {
      val keys = lookupRequests.keySet ++ createRequests.keySet
      debug(s"Lookup for $keys")
      val resolved = storage.findByName(keys.toSeq).map(resolved => (resolved.toQualifiedName, resolved)).toMap
      debug(s"Found $resolved")
      lookupRequests.foreach { tuple =>
        resolved.get(tuple._1) match {
          case Some(rname) =>
            tuple._2.foreach { ref => ref ! Resolved(rname)}
          case None =>
            tuple._2.foreach { ref => ref ! NotFoundName(tuple._1)}
        }
      }
      createRequests.foreach { tuple =>
        resolved.get(tuple._1) match {
          case Some(rname) =>
            tuple._2.foreach { ref => ref ! Resolved(rname)}
          case None =>
            try {
              val result = storage.registerName(tuple._1)
              tuple._2.foreach { ref => ref ! result}
            } catch {
              case ex: UniqueIdRaceException =>
                error("Race: ", ex)
                tuple._2.foreach { ref => ref ! Race(tuple._1)}
              case ex: Throwable =>
                error("UniqueIdActor got error", ex)
                tuple._2.foreach {
                  _ ! ResolutionFailed(ex)
                }
            }
        }
      }
    } finally {
      createRequests = recreate
      lookupRequests = Map()
    }
  }
}

/**
 * @author Andrey Stepachev
 */
class FixedUniqueIdActor(initial: Seq[ResolvedName] = Seq.empty)
  extends Actor with Logging {

  private var ids = initial.map(ev=>(ev.toQualifiedId, ev)).toMap
  private var names = initial.map(ev=>(ev.toQualifiedName, ev)).toMap

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ => Restart
  }

  def add(name: ResolvedName) = {
    if (!names.contains(name.toQualifiedName)) {
      names = names.updated(name.toQualifiedName, name)
      ids = ids.updated(name.toQualifiedId, name)
      debug(s"Adding name $name")
    }
  }

  def receive: Actor.Receive = {

    case r: FindId =>
      ids.get(r.qid) match {
        case Some(x) => sender ! Resolved(x)
        case None => sender ! ResolutionFailed(new IllegalArgumentException(s"Unknown id for request $r"))
      }

    case r: FindName =>
      names.get(r.qname) match {
        case Some(x) => sender ! Resolved(x)
        case None => sender ! ResolutionFailed(new IllegalArgumentException(s"Unknown name for request $r"))
      }
  }
}
