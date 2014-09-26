package popeye.storage.hbase

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.codahale.metrics.MetricRegistry
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import popeye.{Instrumented, Logging}
import popeye.storage.hbase.UniqueIdProtocol._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Success
import HBaseStorage._

trait UniqueId {

  /**
   * Lookup in cache
   * @param qName name to lookup
   * @return optionally id
   */
  def findIdByName(qName: QualifiedName): Option[BytesKey]

  /**
   * Lookup name in cache
   * @param qId id to lookup for
   * @return optionally name
   */
  def findNameById(qId: QualifiedId): Option[String]

  /**
   * Resolve asynchronously id using known name
   * @param qName name to resolve
   * @param create create if not found
   * @return future with id
   */
  def resolveIdByName(qName: QualifiedName,
                      create: Boolean,
                      retries: Int = 3)(implicit timeout: Duration): Future[BytesKey]

  /**
   * Resolve asynchronously name using known id
   * @param qId id to find name for
   * @return future with name
   */
  def resolveNameById(qId: QualifiedId)(implicit timeout: Duration): Future[String]

}

class UniqueIdMetrics(prefix: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val nameCacheMisses = metrics.meter(s"$prefix.cache.name.misses")
  val idCacheMisses = metrics.meter(s"$prefix.cache.id.misses")
  val cacheAdditions = metrics.meter(s"$prefix.cache.additions")
  val creationFailRaces = metrics.meter(s"$prefix.creation.fail.races")
  val resolutionFailures = metrics.meter(s"$prefix.resolution.failures")
}

/**
 * Shared cache for id resolution
 * @author Andrey Stepachev
 */
class UniqueIdImpl(resolver: ActorRef,
                   metrics: UniqueIdMetrics,
                   initialCapacity: Int = 1000,
                   maxCapacity: Int = 100000,
                   timeout: FiniteDuration = 30 seconds)
                  (implicit eCtx: ExecutionContext) extends UniqueId with Logging {

  /** Cache for forward mappings (name to ID). */
  private final val nameCache = new ConcurrentLinkedHashMap.Builder[QualifiedName, Future[BytesKey]]
    .initialCapacity(initialCapacity)
    .maximumWeightedCapacity(maxCapacity)
    .build()

  /** Cache for backward mappings (ID to name).
    * The ID in the key is a byte[] converted to a String to be Comparable. */
  private final val idCache = new ConcurrentLinkedHashMap.Builder[QualifiedId, Future[String]]
    .initialCapacity(initialCapacity)
    .maximumWeightedCapacity(maxCapacity)
    .build()

  /**
   * Lookup in cache
   * @param qName name to lookup
   * @return optionally id
   */
  def findIdByName(qName: QualifiedName): Option[BytesKey] = {
    nameCache.get(qName) match {
      case null =>
        metrics.nameCacheMisses.mark()
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
   * @param qId id to lookup for
   * @return optionally name
   */
  def findNameById(qId: QualifiedId): Option[String] = {
    idCache.get(qId) match {
      case null =>
        metrics.idCacheMisses.mark()
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
   * @param qName name to resolve
   * @param create create if not found
   * @return future with id
   */
  def resolveIdByName(qName: QualifiedName,
                      create: Boolean,
                      retries: Int = 3)(implicit timeout: Duration): Future[BytesKey] = {
    val promise = Promise[BytesKey]()
    nameCache.putIfAbsent(qName, promise.future) match {
      case null =>
        val resolutionFuture =
          resolver.ask(FindName(qName, create))(new Timeout(timeout.toMillis))
        val promiseCompletionFuture = resolutionFuture.map {
          case r: Resolved =>
            addToCache(r)
            promise.success(r.name.id)
          case r: Race =>
            info(s"Got $r, $retries retries left")
            nameCache.remove(qName)
            val idFuture = if (retries == 0) {
              log.error(s"id resolution failed: Can't battle race creating $qName")
              metrics.creationFailRaces.mark()
              Future.failed(new UniqueIdRaceException(s"Can't battle race creating $qName"))
            } else {
              resolveIdByName(qName, create = true, retries - 1)
            }
            promise.completeWith(idFuture)
          case f: ResolutionFailed =>
            log.error("id resolution failed: {}" , f)
            nameCache.remove(qName)
            metrics.resolutionFailures.mark()
            promise.failure(f.t)
          case n: NotFoundName =>
            nameCache.remove(qName)
            promise.failure(new NoSuchElementException(f"no id for name '${n.qname}'"))
        }
        promiseCompletionFuture.onFailure {
          case x: Throwable =>
            log.error("resolution failed", x)
            nameCache.remove(qName)
            promise.failure(x)
        }
        promise.future

      case idFuture => idFuture
    }
  }

  /**
   * Resolve asynchronously name using known id
   * @param qId id to find name for
   * @return future with name
   */
  def resolveNameById(qId: QualifiedId)(implicit timeout: Duration): Future[String] = {
    val promise = Promise[String]()
    idCache.putIfAbsent(qId, promise.future) match {
      case null =>
        val responseFuture =
          resolver.ask(FindId(qId))(new Timeout(timeout.toMillis)).mapTo[Response]
        val promiseCompletionFuture = responseFuture.map {
          case r: Resolved =>
            addToCache(r)
            promise.success(r.name.name)
          case f: ResolutionFailed =>
            log.error("name resolution failed", f.t)
            idCache.remove(qId)
            promise.failure(f.t)
        }
        promiseCompletionFuture.onFailure {
          case x: Throwable =>
            log.error("name resolution failed", x)
            idCache.remove(qId)
            promise.failure(x)
        }
        promise.future
      case idFuture => idFuture
    }
  }

  private def addToCache(r: Resolved) = {
    metrics.cacheAdditions.mark()
    val nameCacheKey = r.name.toQualifiedName
    val idCacheKey = r.name.toQualifiedId
    nameCache.putIfAbsent(nameCacheKey, Promise[BytesKey]().success(r.name.id).future)
    idCache.putIfAbsent(idCacheKey, Promise[String]().success(r.name.name).future)
  }
}
