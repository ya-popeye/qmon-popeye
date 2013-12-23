package popeye.storage.hbase

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import org.apache.hadoop.hbase.util.Bytes
import popeye.Logging
import popeye.storage.hbase.BytesKey._
import popeye.storage.hbase.UniqueIdProtocol._
import scala.Some
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Success
import java.util
import HBaseStorage._

trait UniqueId {

  /**
   * Id width for given UniqueId
   * @return
   */
  def width: Short

  /**
   * Kind name for given UniqueId
   * @return
   */
  def kind: String

  /**
   * Lookup in cache
   * @param name name to lookup
   * @return optionally id
   */
  def findIdByName(name: String): Option[BytesKey]

  /**
   * Lookup name in cache
   * @param id id to lookup for
   * @return optionally name
   */
  def findNameById(id: BytesKey): Option[String]

  /**
   * Resolve asynchronously id using known name
   * @param name name to resolve
   * @param create create if not found
   * @return future with id
   */
  def resolveIdByName(name: String, create: Boolean, retries: Int = 3)(implicit timeout: Duration): Future[BytesKey]

  /**
   * Resolve asynchronously name using known id
   * @param id id to find name for
   * @return future with name
   */
  def resolveNameById(id: BytesKey)(implicit timeout: Duration): Future[String]

  /**
   * Helper method for id <> bytes conversion
   * @param id id to convert to.
   * @return key or exception if result is wider then allowed
   */
  def toBytes(id: Long): BytesKey = {
    val barr = Bytes.toBytes(id)
    for (i <- 0 to (barr.length - width)) {
      if (barr(i) != 0)
        throw new IllegalArgumentException("Id is wider then allowed")
    }
    util.Arrays.copyOfRange(barr, barr.length - width, barr.length)
  }

  /**
   * Helper method for id <> bytes conversion (id expected in big endian order)
   * @param id id to convert to.
   * @return key or exception if result is wider then allowed
   */
  def toId(id: BytesKey, offset: Int, len: Int): Long = {
    if (len != width)
      throw new IllegalArgumentException("Id lenght mismatch")
    var v = 0
    for (i <- Range(id.bytes.length - 1, -1, -1)) {
      v |= id(i)
      v <<= 8
    }
    v
  }
}

/**
 * Shared cache for id resolution
 * @author Andrey Stepachev
 */
class UniqueIdImpl(val width: Short,
               val kind: String,
               resolver: ActorRef,
               initialCapacity: Int = 1000,
               maxCapacity: Int = 100000,
               timeout: FiniteDuration = 30 seconds)
              (implicit eCtx: ExecutionContext) extends UniqueId with Logging {

  /** Cache for forward mappings (name to ID). */
  private final val nameCache = new ConcurrentLinkedHashMap.Builder[String, Future[BytesKey]]
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
  def findIdByName(name: String): Option[BytesKey] = {
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
  def findNameById(id: BytesKey): Option[String] = {
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
  def resolveIdByName(name: String, create: Boolean, retries: Int = 3)(implicit timeout: Duration): Future[BytesKey] = {
    val promise = Promise[BytesKey]()
    nameCache.putIfAbsent(name, promise.future) match {
      case null =>

        val resolutionFuture = resolver.ask(FindName(QualifiedName(kind, name), create))(new Timeout(timeout.toMillis))
        val promiseCompletionFuture = resolutionFuture.map {
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
            log.debug("id resolution failed: {}" , f)
            nameCache.remove(name)
            promise.failure(f.t)
          case n: NotFoundName =>
            nameCache.remove(name)
            promise.failure(new NoSuchElementException(f"no id for name '${n.qname}'"))
        }
        promiseCompletionFuture.onFailure {
          case x: Throwable =>
            log.debug("resolution failed", x)
            nameCache.remove(name)
            promise.failure(x)
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
  def resolveNameById(id: BytesKey)(implicit timeout: Duration): Future[String] = {
    val promise = Promise[String]()
    idCache.putIfAbsent(id, promise.future) match {
      case null =>
        val responseFuture = resolver.ask(FindId(QualifiedId(kind, id)))(new Timeout(timeout.toMillis)).mapTo[Response]
        val promiseCompletionFuture = responseFuture.map {
          case r: Resolved =>
            addToCache(r)
            promise.success(r.name.name)
          case f: ResolutionFailed =>
            idCache.remove(id)
            promise.failure(f.t)
        }
        promiseCompletionFuture.onFailure {
          case x: Throwable =>
            idCache.remove(id)
            promise.failure(x)
        }
        promise.future
      case idFuture => idFuture
    }
  }

  private def addToCache(r: Resolved) = {
    nameCache.putIfAbsent(r.name.name, Promise[BytesKey]().success(r.name.id).future)
    idCache.putIfAbsent(r.name.id, Promise[String]().success(r.name.name).future)
  }
}
