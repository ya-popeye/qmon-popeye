package popeye.storage.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import popeye.Logging
import popeye.storage.hbase.HBaseStorage._
import scala.collection.JavaConversions._
import popeye.storage.hbase.HBaseStorage.QualifiedId
import popeye.storage.hbase.HBaseStorage.QualifiedName
import scala.Some
import popeye.util.hbase.HBaseUtils
import java.util

object UniqueIdStorage {
  final val IdFamily = "id".getBytes
  final val NameFamily = "name".getBytes
  final val MaxIdRow = Array[Byte](0)
}

class UniqueIdStorageException(msg: String, t: Throwable) extends IllegalStateException(msg, t) {
  def this(msg: String) = this(msg, null)
}

class UniqueIdRaceException(msg: String) extends UniqueIdStorageException(msg)

class UniqueIdStorage(tableName: String, hTablePool: HTablePool, kindWidths: Map[String, Short] = UniqueIdMapping) extends Logging {

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
      qname => new Get(qname.name.getBytes(Encoding)).addColumn(IdFamily, Bytes.toBytes(qname.kind))
    }
    withHTable { hTable =>
      for {
        r <- hTable.get(gets) if !r.isEmpty
        k <- r.raw()
      } yield {
        ResolvedName(
          Bytes.toString(k.getQualifier),
          Bytes.toString(k.getRow),
          k.getValue)
      }
    }
  }

  def findByName(qname: QualifiedName): Option[ResolvedName] = {
    withHTable { hTable =>
      val r = hTable.get(new Get(qname.name.getBytes(Encoding)).addColumn(IdFamily, Bytes.toBytes(qname.kind)))
      if (r.isEmpty)
        None
      else
        Some(ResolvedName(
          Bytes.toString(r.raw()(0).getQualifier),
          Bytes.toString(r.raw()(0).getRow),
          r.raw()(0).getValue))
    }
  }

  def findById(ids: Seq[QualifiedId]): Seq[ResolvedName] = {
    val gets = ids.map {
      id => new Get(id.id).addColumn(NameFamily, Bytes.toBytes(id.kind))
    }

    withHTable { hTable =>
      val r = for {
        r <- hTable.get(gets) if !r.isEmpty
        k <- r.raw
      } yield {
        ResolvedName(
          Bytes.toString(k.getQualifier),
          Bytes.toString(k.getValue),
          k.getRow)
      }
      r
    }
  }

  def registerName(qname: QualifiedName): ResolvedName = {
    debug(s"Registering name $qname")
    val idWidth = kindWidths.getOrElse(qname.kind, throw new IllegalArgumentException(s"Unknown kind for $qname"))
    val kindQual = Bytes.toBytes(qname.kind)
    val nameBytes = qname.name.getBytes(Encoding)
    withHTable { hTable =>
      val id = hTable.incrementColumnValue(MaxIdRow, NameFamily, kindQual, 1)
      debug(s"Got id for $qname: $id")
      val idBytes = Bytes.toBytes(id)
      // check, that produced id is not larger, then required id width
      validateLen(idBytes, idWidth)
      val row = java.util.Arrays.copyOfRange(idBytes, idBytes.length - idWidth, idBytes.length).reverse
      if (!cas(hTable, row, NameFamily, kindQual, nameBytes)) {
        val msg = s"Failed assignment: $id -> $qname, already assigned $id, unbelievable"
        log.error(msg)
        throw new UniqueIdStorageException(msg)
      }
      debug(s"Stored reverse mapping for $qname: $id")
      if (!cas(hTable, nameBytes, IdFamily, kindQual, row)) {
        // ok, someone already assigned that name, reuse it
        findByName(qname).getOrElse(throw new IllegalStateException("CAS failed but name not found, something very bad happened"))
      }
      info(s"Registered $qname => $id")
      ResolvedName(qname.kind, qname.name, row)
    }
  }

  private def cas(hTable: HTableInterface, row: Array[Byte], family: Array[Byte], kind: Array[Byte], value: Array[Byte]): Boolean = {
    val put = new Put(row).add(family, kind, value)
    hTable.checkAndPut(row, family, kind, null, put)
  }

  private def validateLen(idBytes: Array[Byte], idWidth: Int) = {
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

  @inline
  private def withHTable[U](body: (HTableInterface) => U): U = {
    val hTable = hTablePool.getTable(tableName)
    try {
      body(hTable)
    } finally {
      hTable.close()
    }
  }

  def getSuggestions(namePrefix: String, kind: String, limit: Int): Seq[String] = {
    require(kindWidths.keys.contains(kind), f"unknown kind: $kind; known kinds: ${kindWidths.keys}")
    val startRow = namePrefix.getBytes(Encoding)
    val endRow = HBaseUtils.addOneIfNotMaximum(startRow)
    val scan = new Scan(startRow, endRow)
    val qualifierSet = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    qualifierSet.add(Bytes.toBytes(kind))
    val familyMap = new util.HashMap[Array[Byte], util.NavigableSet[Array[Byte]]]
    familyMap.put(IdFamily, qualifierSet)
    scan.setFamilyMap(familyMap)
    withHTable {
      table =>
        table.getScanner(scan).next(limit).map {
          result => new String(result.getRow, Encoding)
        }
    }
  }
}
