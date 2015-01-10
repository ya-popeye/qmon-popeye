package popeye.storage.hbase

import akka.actor.{SupervisorStrategy, Actor}
import popeye.Logging
import akka.actor.SupervisorStrategy.Restart
import popeye.storage.{QualifiedId, QualifiedName, ResolvedName}
import scala.collection.mutable
import popeye.storage.hbase.UniqueIdProtocol._
import org.apache.hadoop.hbase.util.Bytes
import popeye.storage.hbase.UniqueIdProtocol.FindName
import popeye.storage.hbase.UniqueIdProtocol.FindId
import popeye.storage.hbase.UniqueIdProtocol.Resolved
import akka.actor.OneForOneStrategy

class InMemoryUniqueIdActor extends Actor with Logging {

  private val idToName = mutable.HashMap[QualifiedId, String]()
  private val nameToId = mutable.HashMap[QualifiedName, BytesKey]()
  private var currentId = 0

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ => Restart
  }

  def receive: Actor.Receive = {

    case r: FindId =>
      idToName.get(r.qid) match {
        case Some(name) => sender ! Resolved(ResolvedName(r.qid, name))
        case None => sender ! NotFoundId(r.qid)
      }

    case r: FindName =>
      nameToId.get(r.qname) match {
        case Some(id) => sender ! Resolved(ResolvedName(r.qname, id))
        case None =>
          if (r.create) {
            currentId += 1
            val array = Array.ofDim[Byte](4)
            Bytes.putInt(array, 0, currentId)
            val id = array.slice(1, 4)
            sender ! Resolved(ResolvedName(r.qname, new BytesKey(id)))
          } else {
            sender ! NotFoundName(r.qname)
          }
      }
  }
}