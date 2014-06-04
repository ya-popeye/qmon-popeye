package popeye.storage.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import popeye.Logging
import popeye.storage.hbase.HBaseStorage._
import scala.collection.JavaConversions._
import popeye.storage.hbase.HBaseStorage.QualifiedId
import popeye.storage.hbase.HBaseStorage.QualifiedName
import scala.collection.JavaConverters._
import popeye.util.hbase.HBaseUtils
import java.util
import org.apache.hadoop.hbase.filter.PrefixFilter

object UniqueIdStorage {
  final val IdFamily = "id".getBytes
  final val NameFamily = "name".getBytes
  final val AssociationsFamily = "assoc".getBytes
  final val MaxIdRow = Array[Byte](0)
}

class UniqueIdStorageException(msg: String, t: Throwable) extends IllegalStateException(msg, t) {
  def this(msg: String) = this(msg, null)
}

class UniqueIdRaceException(msg: String) extends UniqueIdStorageException(msg)

class UniqueIdStorage(tableName: String,
                      hTablePool: HTablePool,
                      namespaceWidth: Int = UniqueIdNamespaceWidth,
                      kindWidths: Map[String, Short] = UniqueIdMapping) extends Logging {

  import UniqueIdStorage._

  def kindWidth(kind: String): Short = {
    kindWidths.getOrElse(kind, throw new IllegalArgumentException(s"Unknown kind $kind"))
  }

  /**
   * @param qnames qualified name to resolve
   * @return resolved names
   */
  def findByName(qnames: Seq[QualifiedName]): Seq[ResolvedName] = {
    val gets = qnames.map {
      qname =>
        val nameRow = createNameRow(qname)
        new Get(nameRow).addColumn(IdFamily, Bytes.toBytes(qname.kind))
    }
    withHTable { hTable =>
      for {
        r <- hTable.get(gets).toSeq if !r.isEmpty
        k <- r.raw().toSeq
      } yield {
        val rowBytes = k.getRow
        val (namespaceBytes, nameBytes) = rowBytes.splitAt(namespaceWidth)
        ResolvedName(
          kind = Bytes.toString(k.getQualifier),
          namespace = new BytesKey(namespaceBytes),
          name = Bytes.toString(nameBytes),
          id = new BytesKey(k.getValue))
      }
    }
  }

  def findByName(qname: QualifiedName): Option[ResolvedName] = findByName(Seq(qname)).headOption

  def findById(ids: Seq[QualifiedId]): Seq[ResolvedName] = {
    val gets = ids.map {
      id =>
        val idRow = id.namespace.bytes ++ id.id.bytes
        new Get(idRow).addColumn(NameFamily, Bytes.toBytes(id.kind))
    }
    withHTable { hTable =>
      val r = for {
        r <- hTable.get(gets) if !r.isEmpty
        k <- r.raw
      } yield {

        val rowBytes = k.getRow
        val (namespaceBytes, idBytes) = rowBytes.splitAt(namespaceWidth)
        ResolvedName(
          kind = Bytes.toString(k.getQualifier),
          namespace = new BytesKey(namespaceBytes),
          name = Bytes.toString(k.getValue),
          id = new BytesKey(idBytes))
      }
      r
    }
  }

  def registerName(qname: QualifiedName): ResolvedName = {
    debug(s"Registering name $qname")
    validateNamespaceLen(qname.namespace)
    val idWidth = kindWidths.getOrElse(qname.kind, throw new IllegalArgumentException(s"Unknown kind for $qname"))
    val kindQual = Bytes.toBytes(qname.kind)
    val nameBytes = qname.name.getBytes(Encoding)
    val namespaceBytes = qname.namespace.bytes
    val nameRow = createNameRow(qname)
    withHTable { hTable =>
      val id = hTable.incrementColumnValue(namespaceBytes ++ MaxIdRow, NameFamily, kindQual, 1)
      debug(s"Got id for $qname: $id")
      val longIdBytes = Bytes.toBytes(id)
      val idBytes = java.util.Arrays.copyOfRange(longIdBytes, longIdBytes.length - idWidth, longIdBytes.length).reverse
      // check, that produced id is not larger, then required id width
      validateIdLen(idBytes, idWidth)
      val idRow = namespaceBytes ++ idBytes
      if (!cas(hTable, idRow, NameFamily, kindQual, nameBytes)) {
        val msg = s"Failed assignment: $id -> $qname, already assigned $id, unbelievable"
        log.error(msg)
        throw new UniqueIdStorageException(msg)
      }
      debug(s"Stored reverse mapping for $qname: $id")
      if (!cas(hTable, nameRow, IdFamily, kindQual, idBytes)) {
        // ok, someone already assigned that name, reuse it
        findByName(qname).getOrElse(throw new IllegalStateException("CAS failed but name not found, something very bad happened"))
      }
      info(s"Registered $qname => $id")
      ResolvedName(qname, idBytes)
    }
  }

  private def cas(hTable: HTableInterface, row: Array[Byte], family: Array[Byte], kind: Array[Byte], value: Array[Byte]): Boolean = {
    val put = new Put(row).add(family, kind, value)
    hTable.checkAndPut(row, family, kind, null, put)
  }

  private def validateIdLen(idBytes: Array[Byte], idWidth: Int) = {
    // check, that produced id is not larger, then required id width
    val maxIndex = idBytes.length - idWidth
    var i = 0
    for (b <- idBytes) {
      if (b != 0 && i < maxIndex) {
        val msg = s"Unable to assign id, " +
          s"all ids depleted (got id ${Bytes.toStringBinary(idBytes)} wider then $idWidth ($i < maxIndex=$maxIndex)}})"
        error(msg)
        throw new IllegalStateException(msg)
      }
      i += 1
    }
  }

  private def validateNamespaceLen(namespace: BytesKey) = {
    require(namespace.bytes.length == namespaceWidth, f"namespace '$namespace' width is not equal to $namespaceWidth")
  }

  private def createNameRow(qname: QualifiedName) = {
    validateNamespaceLen(qname.namespace)
    val nameBytes = qname.name.getBytes(Encoding)
    val namespaceBytes = qname.namespace.bytes
    namespaceBytes ++ nameBytes
  }

  @inline
  private def withHTable[U](body: (HTableInterface) => U): U = {
    val hTable = hTablePool.getTable(tableName)
    try {
      body(hTable)
    } finally {
      hTable.close()
    }
  }

  def getSuggestions(kind: String, namespace: BytesKey, namePrefix: String, limit: Int): Seq[String] = {
    require(kindWidths.keys.contains(kind), f"unknown kind: $kind; known kinds: ${kindWidths.keys}")
    val namePrefixBytes = namePrefix.getBytes(Encoding)
    val startRow = namespace.bytes ++ namePrefixBytes
    val endRow = namespace.bytes ++ HBaseUtils.addOneIfNotMaximum(namePrefixBytes)
    val scan = new Scan(startRow, endRow)
    val qualifierSet = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    qualifierSet.add(Bytes.toBytes(kind))
    val familyMap = new util.HashMap[Array[Byte], util.NavigableSet[Array[Byte]]]
    familyMap.put(IdFamily, qualifierSet)
    scan.setFamilyMap(familyMap)
    withHTable {
      table =>
        table.getScanner(scan).next(limit).toSeq.map {
          result =>
            val rowBytes = result.getRow
            val nameBytesLenght = rowBytes.length - namespaceWidth
            new String(rowBytes, namespaceWidth, nameBytesLenght, Encoding)
        }
    }
  }

  def addRelations(key: QualifiedId, values: Seq[QualifiedId]): Unit = {
    val rowPrefix = key.namespace.bytes ++ Bytes.toBytes(key.kind) ++ key.id.bytes
    val puts = for ((kind, groupedValues) <- values.groupBy(_.kind)) yield {
      val put = new Put(rowPrefix ++ Bytes.toBytes(kind))
      for (value <- groupedValues) {
        require(key.namespace == value.namespace, "generations mismatch")
        put.add(AssociationsFamily, value.id.bytes, Array())
      }
      put
    }
    withHTable {
      table =>
        table.batch(puts.toVector.asJava)
    }
  }

  def getRelations(key: QualifiedId): Seq[QualifiedId] = {
    val rowPrefix = key.namespace.bytes ++ Bytes.toBytes(key.kind) ++ key.id.bytes
    val scan = new Scan(rowPrefix)
    scan.setFilter(new PrefixFilter(rowPrefix))
    val results = withHTable {
      table =>
        table.getScanner(scan).asScala.toBuffer
    }
    results.flatMap {
      result =>
        val row = result.getRow
        val kind = Bytes.toString(row, rowPrefix.length, row.length - rowPrefix.length)
        val assocIds = result.getFamilyMap(AssociationsFamily).asScala.keys
        assocIds.map {
          assocId =>
            QualifiedId(kind, key.namespace, new BytesKey(assocId))
        }
    }
  }
}
