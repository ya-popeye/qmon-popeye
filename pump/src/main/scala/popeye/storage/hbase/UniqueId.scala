package popeye.storage.hbase

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import org.apache.hadoop.hbase.util.Bytes
import popeye.Logging
import popeye.storage.hbase.UniqueId._
import popeye.storage.hbase.UniqueIdProtocol._
import scala.Byte
import scala.Some
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

object UniqueId {

  implicit def bytesToBytesKey(bytes: Array[Byte]) = new BytesKey(bytes)

  /**
   * Helper class, making byte[] comparable
   * @param data what to wrap
   */
  class BytesKey(val data: Array[Byte]) extends Comparable[BytesKey] {

    def compareTo(other: BytesKey): Int = {
      Bytes.BYTES_COMPARATOR.compare(this.data, other.asInstanceOf[UniqueId.BytesKey].data)
    }

    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case other: BytesKey =>
          compareTo(other) == 0
        case _ =>
          throw new IllegalArgumentException("Object of wrong class compared: " + obj.getClass)
      }
    }

    override def hashCode(): Int = {
      Bytes.hashCode(data)
    }
  }

}

/**
 * Shared cache for id resolution
 * @author Andrey Stepachev
 */
class UniqueId(val width: Short,
               val kind: String,
               resolver: ActorRef,
               initialCapacity: Int = 1000,
               maxCapacity: Int = 100000,
               timeout: FiniteDuration = 30 seconds)
              (implicit eCtx: ExecutionContext) extends Logging {

  /** Cache for forward mappings (name to ID). */
  private final val nameCache = new ConcurrentLinkedHashMap.Builder[String, Future[Array[Byte]]]
    .initialCapacity(initialCapacity)
    .maximumWeightedCapacity(maxCapacity)
    .build()

  /** Cache for backward mappings (ID to name).
    * The ID in the key is a byte[] converted to a String to be Comparable. */
  private final val idCache = new ConcurrentLinkedHashMap.Builder[BytesKey, Future[String]]
    .initialCapacity(initialCapacity)
    .maximumWeightedCapacity(maxCapacity)
    .build()

  /**
   * Lookup in cache
   * @param name name to lookup
   * @return optionally id
   */
  def findIdByName(name: String): Option[Array[Byte]] = {
    nameCache.get(name) match {
      case null =>
        None
      case future =>
        future.value match {
          case Some(Success(found)) => Some(found)
          case _ => None
        }
    }
  }

  /**
   * Lookup name in cache
   * @param id id to lookup for
   * @return optionally name
   */
  def findNameById(id: Array[Byte]): Option[String] = {
    idCache.get(id) match {
      case null =>
        None
      case future =>
        future.value match {
          case Some(Success(found)) => Some(found)
          case _ => None
        }
    }
  }

  /**
   * Resolve asynchronously id using known name
   * @param name name to resolve
   * @param create create if not found
   * @return future with id
   */
  def resolveIdByName(name: String, create: Boolean, retries: Int = 3)(implicit timeout: Duration): Future[Array[Byte]] = {
    val promise = Promise[Array[Byte]]()
    nameCache.putIfAbsent(name, promise.future) match {
      case null =>

        val future = resolver.ask(FindName(QualifiedName(kind, name), create))(new Timeout(timeout.toMillis))
        future.onFailure {
          case x: Throwable =>
            nameCache.remove(name)
        }
        future.onSuccess {
          case r: Resolved =>
            addToCache(r)
            promise.success(r.name.id)
          case r: Race =>
            if (retries == 0) {
              nameCache.remove(name)
              promise.failure(new UniqueIdRaceException(s"Can't battle race creating $name"))
            } else {
              info(s"Got $r, $retries retries left")
              nameCache.remove(name) match {
                case null =>
                  promise.completeWith(resolveIdByName(name, create, retries - 1)) // retry
                case removed => removed.value match {
                  case Some(Success(x)) =>
                    promise.success(x)
                  case _ =>
                    promise.completeWith(resolveIdByName(name, create, retries - 1)) // retry
                }
              }

            }
          case f: ResolutionFailed =>
            nameCache.remove(name)
            promise.failure(f.t)
        }
        promise.future
      case idFuture => idFuture
    }
  }

  /**
   * Resolve asynchronously name using known id
   * @param id id to find name for
   * @return future with name
   */
  def resolveNameById(id: Array[Byte])(implicit timeout: Duration): Future[String] = {
    val promise = Promise[String]()
    idCache.putIfAbsent(id, promise.future) match {
      case null =>
        val future = resolver.ask(FindId(QualifiedId(kind, id)))(new Timeout(timeout.toMillis)).mapTo[Resolved]
        future.onComplete {
          case Success(r: Resolved) =>
            addToCache(r)
          case Failure(x) =>
            idCache.remove(id)
        }
        promise.completeWith(future.map {
          resolved =>
            resolved.name.name
        })
        promise.future
      case idFuture => idFuture
    }
  }

  private def addToCache(r: Resolved) = {
    nameCache.putIfAbsent(r.name.name, Promise[Array[Byte]]().success(r.name.id).future)
    idCache.putIfAbsent(r.name.id, Promise[String]().success(r.name.name).future)
  }
}
