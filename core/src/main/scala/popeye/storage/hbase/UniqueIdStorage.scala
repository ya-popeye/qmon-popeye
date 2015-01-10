package popeye.storage.hbase

import com.codahale.metrics.MetricRegistry
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import popeye.{Instrumented, Logging}
import popeye.storage.hbase.HBaseStorage._
import scala.collection.JavaConversions._
import popeye.storage.{QualifiedId, QualifiedName, ResolvedName}
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

class UniqueIdStorageMetrics(name: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val findByNameTimer = metrics.timer(f"$name.find.id.time")
  val findByNameBatchSize = metrics.histogram(f"$name.find.id.batch.size")
  val findByIdTimer = metrics.timer(f"$name.find.name.time")
  val findByIdBatchSize = metrics.histogram(f"$name.find.name.batch.size")
  val registerNameTimer = metrics.timer(f"$name.register.time")
}

class UniqueIdStorage(tableName: String,
                      hTablePool: HTablePool,
                      metrics: UniqueIdStorageMetrics,
                      generationIdWidth: Int = UniqueIdGenerationWidth,
                      kindWidths: Map[String, Short] = UniqueIdMapping) extends Logging {

  import UniqueIdStorage._

  def kindWidth(kind: String): Short = {
    kindWidths.getOrElse(kind, throw new IllegalArgumentException(s"Unknown kind $kind"))
  }

  /**
   * @param qnames qualified name to resolve
   * @return resolved names
   */
  def findByName(qnames: Seq[QualifiedName]): Seq[ResolvedName] = metrics.findByNameTimer.time {
    debug(s"looking up ids by names: ${ qnames.toSeq }")
    metrics.findByNameBatchSize.update(qnames.size)
    val gets = qnames.map {
      qname =>
        val nameRow = createNameRow(qname)
        new Get(nameRow).addColumn(IdFamily, Bytes.toBytes(qname.kind))
    }
    withHTable { hTable =>
      val results = for {
        r <- hTable.get(gets).toSeq if !r.isEmpty
        k <- r.raw().toSeq
      } yield {
        val rowBytes = k.getRow
        val (generationIdBytes, nameBytes) = rowBytes.splitAt(generationIdWidth)
        ResolvedName(
          kind = Bytes.toString(k.getQualifier),
          generationId = new BytesKey(generationIdBytes),
          name = Bytes.toString(nameBytes),
          id = new BytesKey(k.getValue))
      }
      debug(s"looking up ids by names, results: ${ results.toSeq }")
      results
    }
  }

  def findByName(qname: QualifiedName): Option[ResolvedName] = findByName(Seq(qname)).headOption

  def findById(ids: Seq[QualifiedId]): Seq[ResolvedName] = metrics.findByIdTimer.time {
    debug(s"looking up names by ids: ${ ids.toSeq }")
    metrics.findByIdBatchSize.update(ids.size)
    val gets = ids.map {
      id =>
        val idRow = id.generationId.bytes ++ id.id.bytes
        new Get(idRow).addColumn(NameFamily, Bytes.toBytes(id.kind))
    }
    withHTable { hTable =>
      val r = for {
        r <- hTable.get(gets) if !r.isEmpty
        k <- r.raw
      } yield {

        val rowBytes = k.getRow
        val (generationIdBytes, idBytes) = rowBytes.splitAt(generationIdWidth)
        ResolvedName(
          kind = Bytes.toString(k.getQualifier),
          generationId = new BytesKey(generationIdBytes),
          name = Bytes.toString(k.getValue),
          id = new BytesKey(idBytes))
      }
      debug(s"looking up names by ids, results: ${ r.toSeq }")
      r
    }
  }

  def registerName(qname: QualifiedName): ResolvedName = metrics.registerNameTimer.time {
    debug(s"Registering name $qname")
    validateGenerationIdLen(qname.generationId)
    val idWidth = kindWidths.getOrElse(qname.kind, throw new IllegalArgumentException(s"Unknown kind for $qname"))
    val kindQual = Bytes.toBytes(qname.kind)
    val nameBytes = qname.name.getBytes(Encoding)
    val generationIdBytes = qname.generationId.bytes
    val nameRow = createNameRow(qname)
    withHTable { hTable =>
      val id = hTable.incrementColumnValue(generationIdBytes ++ MaxIdRow, NameFamily, kindQual, 1)
      debug(s"Got id for $qname: $id")
      val longIdBytes = Bytes.toBytes(id)
      val idBytes = java.util.Arrays.copyOfRange(longIdBytes, longIdBytes.length - idWidth, longIdBytes.length).reverse
      // check, that produced id is not larger, then required id width
      validateIdLen(idBytes, idWidth)
      val idRow = generationIdBytes ++ idBytes
      if (!cas(hTable, idRow, NameFamily, kindQual, nameBytes)) {
        val msg = s"Failed assignment: $id -> $qname, already assigned $id, unbelievable"
        log.error(msg)
        throw new UniqueIdStorageException(msg)
      }
      debug(s"Stored reverse mapping for $qname: $id")
      if (!cas(hTable, nameRow, IdFamily, kindQual, idBytes)) {
        // ok, someone already assigned that name, reuse it
        val id = findByName(qname).getOrElse {
          throw new IllegalStateException("CAS failed but name not found, something very bad happened")
        }
        return id
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

  private def validateGenerationIdLen(generationId: BytesKey) = {
    require(generationId.bytes.length == generationIdWidth, f"generationId '$generationId' width is not equal to $generationIdWidth")
  }

  private def createNameRow(qname: QualifiedName) = {
    validateGenerationIdLen(qname.generationId)
    val nameBytes = qname.name.getBytes(Encoding)
    qname.generationId.bytes ++ nameBytes
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

  def getSuggestions(kind: String, generationId: BytesKey, namePrefix: String, limit: Int): Seq[String] = {
    require(kindWidths.keys.contains(kind), f"unknown kind: $kind; known kinds: ${kindWidths.keys}")
    val namePrefixBytes = namePrefix.getBytes(Encoding)
    val startRow = generationId.bytes ++ namePrefixBytes
    val endRow = generationId.bytes ++ HBaseUtils.addOneIfNotMaximum(namePrefixBytes)
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
            val nameBytesLenght = rowBytes.length - generationIdWidth
            new String(rowBytes, generationIdWidth, nameBytesLenght, Encoding)
        }
    }
  }
}
