package popeye.storage.hbase

import akka.actor.{OneForOneStrategy, SupervisorStrategy, Actor}
import popeye.Logging
import akka.actor.SupervisorStrategy.Restart
import popeye.storage.hbase.HBaseStorage.{QualifiedName, QualifiedId, ResolvedName}
import scala.collection.mutable
import popeye.storage.hbase.UniqueIdProtocol.{ResolutionFailed, Resolved, FindName, FindId}
import org.apache.hadoop.hbase.util.Bytes

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
        case None => sender ! ResolutionFailed(new IllegalArgumentException(s"Unknown id for request $r"))
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
            sender ! ResolutionFailed(new IllegalArgumentException(s"Unknown name for request $r"))
          }
      }
  }
}