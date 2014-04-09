package popeye.javaapi.storage.hbase

import popeye.storage.hbase.{UniqueIdStorage => ScalaUniqueIdStorage, BytesKey}
import org.apache.hadoop.hbase.client.HTablePool
import popeye.storage.hbase.HBaseStorage._
import scala.collection.JavaConverters._

class UniqueIdStorage(tableName: String, hTablePool: HTablePool) {
  val scalaStorage = new ScalaUniqueIdStorage(tableName, hTablePool)

  def findByNames(names: java.util.Set[QualifiedName]): java.util.Map[QualifiedName, BytesKey] = {
    val scalaNames = names.asScala.toSeq
    val ids = scalaStorage.findByName(scalaNames).toSeq.map(_.id)
    val scalaMap: Map[QualifiedName, BytesKey] = scalaNames.zip(ids)(scala.collection.breakOut)
    scalaMap.asJava
  }
}
