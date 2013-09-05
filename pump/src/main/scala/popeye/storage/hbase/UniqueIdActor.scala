package popeye.storage.hbase

import akka.actor.{ActorRef, Actor}
import popeye.Logging
import popeye.storage.hbase.UniqueIdProtocol._
import popeye.storage.hbase.UniqueId._
import popeye.storage.hbase.UniqueIdProtocol.NotFoundName
import popeye.storage.hbase.UniqueIdProtocol.FindName
import popeye.storage.hbase.UniqueIdProtocol.Resolved
import scala.Some
import popeye.storage.hbase.UniqueIdProtocol.FindId
import akka.actor.Terminated

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
      val resolved = storage.findByName(keys.toSeq).map(resolved => (resolved.toQualifiedName, resolved)).toMap
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
              storage.registerName(tuple._1)
            } catch {
              case ex: UniqueIdRaceException =>
                tuple._2.foreach { ref => ref ! Race(tuple._1)}
              case ex: Throwable =>
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
