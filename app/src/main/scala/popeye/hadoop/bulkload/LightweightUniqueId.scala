package popeye.hadoop.bulkload

import popeye.storage.{QualifiedName, ResolvedName}
import popeye.storage.hbase.{UniqueIdStorage, UniqueIdStorageException, BytesKey}
import scala.util.Try
import java.util
import scala.collection.mutable

class LightweightUniqueId(idStorage: UniqueIdStorage, maxCacheSize: Int) {

  private val idCache = createIdCache(maxCacheSize)

  def findByName(name: QualifiedName): Option[BytesKey] = idCache.get(name)

  def findOrRegisterIdsByNames(names: Set[QualifiedName]): Map[QualifiedName, BytesKey] = {
    val namesSeq = names.toSeq
    val oldIdsMap = toIdsMap(idStorage.findByName(namesSeq).toSeq)
    val nonExistentNames = namesSeq.filterNot(oldIdsMap.contains)
    val newIdsMap = toIdsMap(nonExistentNames.map(registerName))
    val idsMap = oldIdsMap ++ newIdsMap
    idCache ++= idsMap
    idsMap
  }

  private def toIdsMap(names: Seq[ResolvedName]): Map[QualifiedName, BytesKey] =
    names.map(resolvedName => (resolvedName.toQualifiedName, resolvedName.id)).toMap

  private def registerName(name: QualifiedName): ResolvedName = {
    val attempt = Try(idStorage.registerName(name))
    attempt.recoverWith {
      case e: UniqueIdStorageException => Try {
        idStorage.findByName(Seq(name)).headOption
          .getOrElse(throw new RuntimeException("name registration failed, but it wasn't due to a race", e))
      }
    }.get
  }

  private def createIdCache(capacity: Int): mutable.Map[QualifiedName, BytesKey] = {
    import scala.collection.JavaConverters._
    val javaMap = new util.LinkedHashMap[QualifiedName, BytesKey] {
      protected override def removeEldestEntry(eldest: java.util.Map.Entry[QualifiedName, BytesKey]): Boolean = {
        size > capacity + 1
      }
    }
    javaMap.asScala
  }
}
