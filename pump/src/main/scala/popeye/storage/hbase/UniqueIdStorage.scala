package popeye.storage.hbase

import org.apache.hadoop.hbase.client.{HTablePool, Get, Put, HTableInterface}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
import popeye.Logging
import popeye.storage.hbase.HBaseStorage._
import popeye.storage.hbase.UniqueIdStorage._
import akka.actor.{ActorRef, Props}

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
    kindWidths.getOrElse(kind, throw new IllegalArgumentException(s"Unknwon kind $kind"))
  }

  /**
   * @param qnames qualified name to resolve
   * @return resolved names
   */
  def findByName(qnames: Seq[QualifiedName]): Seq[ResolvedName] = {
    val gets = qnames.map {
      qname => new Get(qname.name.getBytes(Encoding)).addColumn(NameFamily, Bytes.toBytes(qname.kind))
    }
    withHTable { hTable =>
      for (
        r <- hTable.get(gets);
        k <- r.raw()
      ) yield {
        ResolvedName(
          Bytes.toString(k.getQualifier),
          Bytes.toString(k.getValue),
          k.getRow)
      }
    }
  }

  def findByName(qname: QualifiedName): Option[ResolvedName] = {
    withHTable{ hTable =>
      val r = hTable.get(new Get(qname.name.getBytes(Encoding)).addColumn(NameFamily, Bytes.toBytes(qname.kind)))
      if (r.isEmpty)
        None
      else
        Some(ResolvedName(
          Bytes.toString(r.raw()(0).getQualifier),
          Bytes.toString(r.raw()(0).getValue),
          r.raw()(0).getRow))
    }
  }

  def findById(ids: Seq[QualifiedId]): Seq[ResolvedName] = {
    val gets = ids.map {
      id => new Get(id.id).addColumn(IdFamily, Bytes.toBytes(id.kind))
    }

    withHTable{ hTable =>
      val r = for (
        r <- hTable.get(gets);
        k <- r.raw
      ) yield {
        ResolvedName(
          Bytes.toString(k.getQualifier),
          Bytes.toString(k.getValue),
          k.getRow)
      }
      r
    }
  }

  def registerName(qname: QualifiedName): ResolvedName = {
    val idWidth = kindWidths.getOrElse(qname.kind, throw new IllegalArgumentException(s"Unknwon kind for $qname"))
    val kindQual = Bytes.toBytes(qname.kind)
    val nameBytes = qname.name.getBytes(Encoding)
    withHTable{ hTable =>
      val id = hTable.incrementColumnValue(MaxIdRow, NameFamily, kindQual, 1)
      val idBytes = Bytes.toBytes(id)
      // check, that produced id is not larger, then required id width
      for (i <- Range(0, 7)) {
        if (idBytes(i) != '0' && i > idWidth)
          throw new IllegalStateException(s"Unable to assign id, " +
            s"all ids depleted (got id $id which is wider then $idWidth)")
      }
      val row = java.util.Arrays.copyOfRange(idBytes, idBytes.length - idWidth, idBytes.length)
      if (!cas(hTable, row, NameFamily, kindQual, nameBytes)) {
        val msg = s"Failed assignment: $id -> $qname, already assigned $id, unbelievable"
        log.error(msg)
        throw new UniqueIdStorageException(msg)
      }
      if (!cas(hTable, nameBytes, IdFamily, kindQual, row)) {
        // ok, someone already assigned that name, reuse it
        findByName(qname).getOrElse(throw new IllegalStateException("CAS failed but name not found, something very bad happened"))
      }
      ResolvedName(qname.kind, qname.name, idBytes)
    }
  }

  private def cas(hTable: HTableInterface, row: Array[Byte], family: Array[Byte], kind: Array[Byte], value: Array[Byte]): Boolean = {
    val put = new Put(row).add(NameFamily, kind, value)
    hTable.checkAndPut(row, NameFamily, kind, null, put)
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
}
