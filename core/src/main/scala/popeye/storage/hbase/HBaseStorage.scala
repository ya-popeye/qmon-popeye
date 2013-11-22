package popeye.storage.hbase

import com.codahale.metrics.MetricRegistry
import java.util
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Put, HTablePool}
import org.apache.hadoop.hbase.util.Bytes
import popeye.storage.hbase.HBaseStorage._
import popeye.proto.{PackedPoints, Message}
import popeye.{Instrumented, Logging}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import com.typesafe.config.Config
import akka.actor.{ActorRef, Props, ActorSystem}
import java.util.concurrent.TimeUnit
import popeye.util.hbase.HBaseConfigured
import popeye.pipeline.{PointsSinkFactory, PipelineSinkFactory, PointsSink}
import java.nio.charset.Charset

object HBaseStorage {
  final val Encoding = Charset.forName("UTF-8")

  /** Number of bytes on which a timestamp is encoded.  */
  final val TIMESTAMP_BYTES: Short = 4
  /** Maximum number of tags allowed per data point.  */
  final val MAX_NUM_TAGS: Short = 8
  /** Number of LSBs in time_deltas reserved for flags.  */
  final val FLAG_BITS: Short = 4
  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  final val FLAG_FLOAT: Short = 0x8
  /** Mask to select the size of a value from the qualifier.  */
  final val LENGTH_MASK: Short = 0x7
  /** Mask to select all the FLAG_BITS.  */
  final val FLAGS_MASK: Short = (FLAG_FLOAT | LENGTH_MASK).toShort
  /** Max time delta (in seconds) we can store in a column qualifier.  */
  final val MAX_TIMESPAN: Short = 3600
  /**
   * Array containing the hexadecimal characters (0 to 9, A to F).
   * This array is read-only, changing its contents leads to an undefined
   * behavior.
   */
  final val HEX: Array[Byte] = Array[Byte]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

  final val PointsFamily = "t".getBytes(Encoding)

  final val MetricKind: String = "metric"
  final val AttrNameKind: String = "tagk"
  final val AttrValueKind: String = "tagv"

  final val UniqueIdMapping = Map[String, Short](
    MetricKind -> 3.toShort,
    AttrNameKind -> 3.toShort,
    AttrValueKind -> 3.toShort
  )

  sealed case class ResolvedName(kind: String, name: String, id: BytesKey) {
    def this(qname: QualifiedName, id: BytesKey) = this(qname.kind, qname.name, id)

    def this(qid: QualifiedId, name: String) = this(qid.kind, name, qid.id)

    def toQualifiedName = QualifiedName(kind, name)

    def toQualifiedId = QualifiedId(kind, id)
  }

  object ResolvedName {
    def apply(qname: QualifiedName, id: BytesKey) = new ResolvedName(qname.kind, qname.name, id)

    def apply(qid: QualifiedId, name: String) = new ResolvedName(qid.kind, name, qid.id)
  }

  sealed case class QualifiedName(kind: String, name: String)

  sealed case class QualifiedId(kind: String, id: BytesKey)

  def sinkFactory(): PipelineSinkFactory = new HBasePipelineSinkFactory
}

class HBasePointsSink(config: Config, storage: HBaseStorage)(implicit eCtx: ExecutionContext) extends PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
    storage.writePoints(points)(eCtx).mapTo[Long]
  }
}


case class HBaseStorageMetrics(name: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeProcessingTime = metrics.timer(s"$name.storage.write.processing.time")
  val writeHBaseTime = metrics.timer(s"$name.storage.write.hbase.time")
  val writeTime = metrics.timer(s"$name.storage.write.time")
  val writeHBasePoints = metrics.meter(s"$name.storage.write.points")
  val readProcessingTime = metrics.timer(s"$name.storage.read.processing.time")
  val readHBaseTime = metrics.timer(s"$name.storage.read.hbase.time")
  val resolvedPointsMeter = metrics.meter(s"$name.storage.resolved.points")
  val delayedPointsMeter = metrics.meter(s"$name.storage.delayed.points")
}

class HBaseStorage(tableName: String,
                    hTablePool: HTablePool,
                    metricNames: UniqueId,
                    attributeNames: UniqueId,
                    attributeValues: UniqueId,
                    metrics: HBaseStorageMetrics,
                    resolveTimeout: Duration = 15 seconds) extends Logging {

  val tableBytes = tableName.getBytes(Encoding)

  def ping(): Unit = {
    Await.result(metricNames.resolveIdByName("_.ping", create = true)(resolveTimeout), resolveTimeout)
  }
  /**
   * Write points, returned future is awaitable, but can be already completed
   * @param points what to write
   * @return number of written points
   */
  def writePoints(points: PackedPoints)(implicit eCtx: ExecutionContext): Future[Int] = {

    val ctx = metrics.writeTime.timerContext()
    metrics.writeProcessingTime.time {
      val delayedKeyValues = mutable.Buffer[Future[KeyValue]]()
      val keyValues = mutable.Buffer[KeyValue]()

      // resolve identificators
      // unresolved will be delayed for furure expansion
      points.foreach { point =>
        val m = metricNames.findIdByName(point.getMetric)
        val a = point.getAttributesList.sortBy(_.getName).map { attr =>
          (attributeNames.findIdByName(attr.getName),
            attributeValues.findIdByName(attr.getValue))
        }
        if (m.isEmpty || a.exists(a => a._1.isEmpty || a._2.isEmpty))
          delayedKeyValues += mkKeyValueFuture(point)
        else
          keyValues += mkKeyValue(m.get,
            a.map { t => (t._1.get, t._2.get)},
            point.getTimestamp,
            mkQualifiedValue(point))
      }

      // write resolved points
      val writeComplete = if (!keyValues.isEmpty) Future[Int] {
        writeKv(keyValues)
        keyValues.size
      } else Future.successful[Int](0)

      metrics.resolvedPointsMeter.mark(keyValues.size)
      metrics.delayedPointsMeter.mark(delayedKeyValues.size)

      // if we have some unresolved keyvalues, combine them to composite feature,
      // in case of no delayed kevalues, simply return write-feature
      if (delayedKeyValues.size > 0) {
        val delayedFuture = Future.sequence(delayedKeyValues).map { kvl =>
          writeKv(kvl)
          kvl.size
        }
        Future.reduce(Seq(delayedFuture, writeComplete)){(a, b) =>
          ctx.stop
          a+b
        }
      } else {
        writeComplete.map { a => ctx.stop; a}
      }
    }
  }

  private def writeKv(kvList: Seq[KeyValue]) = {
    metrics.writeHBaseTime.time {
      debug(s"Making puts for ${kvList.size} keyvalues")
      val puts = new util.ArrayList[Put](kvList.length)
      kvList.foreach { k =>
        puts.add(new Put(k.getRow).add(k))
      }
      withDebug {
        val l = puts.map(_.heapSize()).foldLeft(0l)(_ + _)
        debug(s"Writing ${kvList.size} keyvalues (heapsize=$l)")
      }
      val hTable = hTablePool.getTable(tableName)
      hTable.setAutoFlush(false, true)
      hTable.setWriteBufferSize(4*1024*1024)
      try {
        hTable.batch(puts)
        debug(s"Writing ${kvList.size} keyvalues - flushing")
        hTable.flushCommits()
        debug(s"Writing ${kvList.size} keyvalues - done")
        metrics.writeHBasePoints.mark(puts.size())
      } catch {
        case e: Exception => error("Failed to write points", e)
      } finally {
        hTable.close()
      }
    }
  }

  private def mkKeyValue(metric: BytesKey, attrs: Seq[(BytesKey, BytesKey)], timestamp: Long, value: (Array[Byte], Array[Byte])) = {
    val baseTime: Int = (timestamp - (timestamp % HBaseStorage.MAX_TIMESPAN)).toInt
    val row = new Array[Byte](metricNames.width + HBaseStorage.TIMESTAMP_BYTES +
      attrs.length * (attributeNames.width + attributeValues.width))
    var off = 0
    off = copyBytes(metric, row, off)
    off = copyBytes(Bytes.toBytes(baseTime.toInt), row, off)
    for (attr <- attrs) {
      off = copyBytes(attr._1, row, off)
      off = copyBytes(attr._2, row, off)
    }

    val delta = (timestamp - baseTime).toShort
    trace(s"Made point: ts=$timestamp, basets=$baseTime, delta=$delta")
    new KeyValue(row, HBaseStorage.PointsFamily, value._1, value._2)
  }

  /**
   * Produce tuple with qualifier and value represented as byte arrays
   * @param point point to pack
   * @return packed (qualifier, value)
   */
  private def mkQualifiedValue(point: Message.Point): (Array[Byte], Array[Byte]) = {
    val delta: Short = (point.getTimestamp % HBaseStorage.MAX_TIMESPAN).toShort
    val ndelta = delta << HBaseStorage.FLAG_BITS
    if (point.hasFloatValue)
      (Bytes.toBytes((0xffff & (HBaseStorage.FLAG_FLOAT | 0x3) & ndelta).toShort),
        Bytes.toBytes(point.getFloatValue))
    else if (point.hasIntValue)
      (Bytes.toBytes((0xffff & ndelta).toShort), Bytes.toBytes(point.getIntValue))
    else
      throw new IllegalArgumentException("Neither int nor float values set on point")

  }

  private def mkPoint(baseTime: Int, metric: String,
                      qualifier: Short, value: Array[Byte],
                      attrs: Seq[(String, String)]): Message.Point = {
    val builder = Message.Point.newBuilder()
      .setMetric(metric)
    if ((qualifier & HBaseStorage.FLAG_FLOAT) == 0) {
      // int
      builder.setIntValue(Bytes.toLong(value))
    } else {
      // float
      builder.setFloatValue(Bytes.toFloat(value))
    }

    val deltaTs = delta(qualifier)
    builder.setTimestamp(baseTime + deltaTs)

    trace(s"Got point: ts=${builder.getTimestamp}, qualifier=$qualifier basets=$baseTime, delta=$deltaTs")

    attrs.foreach { attribute =>
      builder.addAttributesBuilder()
        .setName(attribute._1)
        .setValue(attribute._2)
    }
    builder.build
  }

  /**
   * Makes Future for KeyValue.
   * Most time all identifiers are cached, so this future returns 'complete',
   * but in case of some unresolved identifiers future will be incomplete
   *
   * Attributes are written in sorted-by-string-name order.
   *
   * @param point point to resolve
   * @return future
   */
  private def mkKeyValueFuture(point: Message.Point)(implicit eCtx: ExecutionContext): Future[KeyValue] = {
    val metricIdFuture = metricNames.resolveIdByName(point.getMetric, create = true)(resolveTimeout)
    val attributes = Future.traverse(point.getAttributesList.sortBy(_.getName)) {
      attribute =>
        attributeNames.resolveIdByName(attribute.getName, create = true)(resolveTimeout)
          .zip(attributeValues.resolveIdByName(attribute.getValue, create = true)(resolveTimeout))
    }
    metricIdFuture.zip(attributes).map {
      case tuple =>
        mkKeyValue(tuple._1, tuple._2, point.getTimestamp, mkQualifiedValue(point))
    }
  }

  def pointToKeyValue(point: Message.Point)(implicit eCtx: ExecutionContext): KeyValue = {
    Await.result(mkKeyValueFuture(point), resolveTimeout)
  }

  /**
   * Makes Future for Message.Point from KeyValue.
   * Most time all identifiers are cached, so this future returns 'complete',
   * but in case of some unresolved identifiers future will be incomplete and become asynchronous
   *
   * @param kv keyvalue to restore
   * @return future
   */
  private def mkPointFuture(kv: KeyValue)(implicit eCtx: ExecutionContext): Future[Message.Point] = {
    val row = kv.getRow
    val kvValue = kv.getValue
    val kvQualifier = kv.getQualifier
    require(kv.getRowLength >= metricNames.width,
      s"Metric Id is wider then row key ${Bytes.toStringBinary(row)}")
    require(kv.getQualifierLength == Bytes.SIZEOF_SHORT,
      s"Expected qualifier to be with size of Short")
    val attributesLen = row.length - metricNames.width - HBaseStorage.TIMESTAMP_BYTES
    val attributeLen = attributeNames.width + attributeValues.width
    require(attributesLen %
      (attributeNames.width + attributeValues.width) == 0,
      s"Row key should have room for" +
        s" metric name (${metricNames.width} bytes) +" +
        s" timestamp (${HBaseStorage.TIMESTAMP_BYTES}} bytes)}" +
        s" qualifier (N x $attributeLen bytes)}" +
        s" but got row ${Bytes.toStringBinary(row)}")


    val metricNameFuture = metricNames.resolveNameById(util.Arrays.copyOf(kv.getRow, metricNames.width))(resolveTimeout)
    var atOffset = metricNames.width + HBaseStorage.TIMESTAMP_BYTES
    val attributes = Future.traverse((0 until attributesLen / attributeLen)
      .map { idx =>
      val atId = util.Arrays.copyOfRange(row, atOffset, atOffset + attributeNames.width)
      val atVal = util.Arrays.copyOfRange(row,
        atOffset + attributeNames.width,
        atOffset + attributeNames.width + attributeValues.width)
      atOffset += attributeLen * idx
      (atId, atVal)
    }) { attribute =>
      attributeNames.resolveNameById(attribute._1)(resolveTimeout)
        .zip(attributeValues.resolveNameById(attribute._2)(resolveTimeout))
    }
    metricNameFuture.zip(attributes).map {
      case tuple =>
        val baseTs = Bytes.toInt(row, metricNames.width, HBaseStorage.TIMESTAMP_BYTES)
        mkPoint(baseTs, tuple._1, Bytes.toShort(kvQualifier), kvValue, tuple._2)
    }
  }

  def keyValueToPoint(kv: KeyValue)(implicit eCtx: ExecutionContext): Message.Point = {
    Await.result(mkPointFuture(kv), resolveTimeout)
  }

  private def delta(qualifier: Short) = {
    ((qualifier & 0xFFFF) >>> HBaseStorage.FLAG_BITS).toShort
  }

  @inline
  private def copyBytes(src: Array[Byte], dst: Array[Byte], off: Int): Int = {
    System.arraycopy(src, 0, dst, off, src.length)
    off + src.length
  }

}

class HBaseStorageConfig(val config: Config,
                          val actorSystem: ActorSystem,
                          val metricRegistry: MetricRegistry,
                          val storageName: String = "hbase")
                         (implicit val eCtx: ExecutionContext){
  val uidsTableName = config.getString("table.uids")
  val pointsTableName = config.getString("table.points")
  val poolSize = config.getInt("pool.max")
  val zkQuorum = config.getString("zk.quorum")
  val resolveTimeout = new FiniteDuration(config.getMilliseconds(s"uids.resolve-timeout"), TimeUnit.MILLISECONDS)
  val uidsConfig = config.getConfig("uids")
}

/**
 * Encapsulates configured hbase client and points storage actors.
 * @param config provides necessary configuration parameters
 */
class HBaseStorageConfigured(config: HBaseStorageConfig) {

  val hbase = new HBaseConfigured(
    config.config, config.zkQuorum, config.poolSize)

  config.actorSystem.registerOnTermination(hbase.close())

  val storage = {
    implicit val eCtx = config.eCtx

    val uniqueIdStorage = new UniqueIdStorage(config.uidsTableName, hbase.hTablePool, HBaseStorage.UniqueIdMapping)
    val uniqIdResolved = config.actorSystem.actorOf(Props.apply(new UniqueIdActor(uniqueIdStorage)))
    val metrics = makeUniqueIdCache(config.uidsConfig, HBaseStorage.MetricKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    val attrNames = makeUniqueIdCache(config.uidsConfig, HBaseStorage.AttrNameKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    val attrValues = makeUniqueIdCache(config.uidsConfig, HBaseStorage.AttrValueKind, uniqIdResolved, uniqueIdStorage,
      config.resolveTimeout)
    new HBaseStorage(
      config.pointsTableName, hbase.hTablePool, metrics, attrNames, attrValues,
      new HBaseStorageMetrics(config.storageName, config.metricRegistry), config.resolveTimeout)
  }

  private def makeUniqueIdCache(config: Config, kind: String, resolver: ActorRef,
                                storage: UniqueIdStorage, resolveTimeout: FiniteDuration)
                               (implicit eCtx: ExecutionContext): UniqueId = {
    new UniqueIdImpl(storage.kindWidth(kind), kind, resolver,
      config.getInt(s"$kind.initial-capacity"),
      config.getInt(s"$kind.max-capacity"),
      resolveTimeout
    )
  }

}