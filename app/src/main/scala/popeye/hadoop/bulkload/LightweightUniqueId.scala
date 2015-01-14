package popeye.hadoop.bulkload

import popeye.storage.{QualifiedId, ResolvedName, QualifiedName}
import popeye.storage.hbase.{UniqueIdStorage, UniqueIdStorageException, BytesKey}
import scala.util.Try
import java.util
import scala.collection.mutable

class LightweightUniqueId(idStorage: UniqueIdStorage, maxCacheSize: Int) {

  private val idCache = createCache[QualifiedName, BytesKey](maxCacheSize)
  private val nameCache = createCache[QualifiedId, String](maxCacheSize)

  def findByName(name: QualifiedName): Option[BytesKey] = idCache.get(name)

  def findOrRegisterIdsByNames(names: Set[QualifiedName]): Map[QualifiedName, BytesKey] = {
    val namesSeq = names.toSeq
    val nameAndIdOptions = namesSeq.map(name => (name, findByName(name)))
    val nonCachedNames = nameAndIdOptions.collect { case (name, None) => name}
    val cachedIdsMap = nameAndIdOptions.collect { case (name, Some(id)) => (name, id)}.toMap
    val existentIdsMap = toIdsMap(idStorage.findByName(nonCachedNames))
    val nonExistentNames = nonCachedNames.filterNot(existentIdsMap.contains)
    val newIdsMap = toIdsMap(nonExistentNames.map(registerName))
    val idsMap = cachedIdsMap ++ existentIdsMap ++ newIdsMap
    idCache ++= idsMap
    idsMap
  }

  def findById(qId: QualifiedId) = nameCache.get(qId)

  def resolveByIds(qIds: Set[QualifiedId]): Map[QualifiedId, String] = {
    val idAndNameOptions = qIds.map(id => (id, findById(id)))
    val nonCachedIds = idAndNameOptions.collect { case (id, None) => id}
    val cachedNames = idAndNameOptions.collect { case (id, Some(name)) => (id, name)}.toMap
    val idsSeq = nonCachedIds.toSeq
    val namesMap = toNamesMap(idStorage.findById(idsSeq))
    nameCache ++= namesMap
    cachedNames ++ namesMap
  }

  private def toIdsMap(names: Seq[ResolvedName]): Map[QualifiedName, BytesKey] =
    names.map(resolvedName => (resolvedName.toQualifiedName, resolvedName.id)).toMap

  private def toNamesMap(names: Seq[ResolvedName]): Map[QualifiedId, String] =
    names.map(resolvedName => (resolvedName.toQualifiedId, resolvedName.name)).toMap

  private def registerName(name: QualifiedName): ResolvedName = {
    val attempt = Try(idStorage.registerName(name))
    attempt.recoverWith {
      case e: UniqueIdStorageException => Try {
        idStorage.findByName(Seq(name)).headOption
          .getOrElse(throw new RuntimeException("name registration failed, but it wasn't due to a race", e))
      }
    }.get
  }

  private def createCache[A, B](capacity: Int): mutable.Map[A, B] = {
    import scala.collection.JavaConverters._
    val javaMap = new util.LinkedHashMap[A, B] {
      protected override def removeEldestEntry(eldest: java.util.Map.Entry[A, B]): Boolean = {
        size > capacity + 1
      }
    }
    javaMap.asScala
  }
}
