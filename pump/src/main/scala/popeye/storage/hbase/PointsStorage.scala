package popeye.storage.hbase

import java.util
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Put, HTablePool}
import org.apache.hadoop.hbase.util.Bytes
import popeye.storage.hbase.HBaseStorage._
import popeye.transport.proto.Message
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import popeye.Logging
import scala.collection.mutable
import scala.util.{Failure, Success}


/**
 * @author Andrey Stepachev
 */
class PointsStorage(tableName: String,
                    hTablePool: HTablePool,
                    metricNames: UniqueId,
                    attributeNames: UniqueId,
                    attributeValues: UniqueId,
                    resolveTimeout: Duration = 15 seconds) extends Logging {
  val tableBytes = tableName.getBytes(Encoding)

  class KeyValueBuilder {

  }

  /**
   * Write points, returned future is awaitable, but can be already completed
   * @param points what to write
   * @return number of written points
   */
  def writePoints(points: Seq[Message.Point])(implicit eCtx: ExecutionContext): Future[Int] = {

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
      if (m.isEmpty || a.forall{a=>a._1.isEmpty || a._2.isEmpty})
        delayedKeyValues += mkKeyValueFuture(point)
      else
        keyValues += mkKeyValue(m.get,
          a.map{t=>(t._1.get, t._2.get)},
          point.getTimestamp,
          mkQualifiedValue(point))
    }

    // write resolved points
    val writeComplete = if (!keyValues.isEmpty) Future[Int]{
      writeKv(keyValues)
      keyValues.size
    } else Future.successful[Int](0)

    // if we have some unresolved keyvalues, combine them to composite feature,
    // in case of no delayed kevalues, simply return write-feature
    if (delayedKeyValues.size > 0) {
      val delayedFuture = Future.sequence(delayedKeyValues).map { kvl =>
        writeKv(kvl)
        kvl.size
      }
      Future.reduce(Seq(delayedFuture, writeComplete))((a,b) =>
        a+b
      )
    } else {
      writeComplete
    }
  }

  private def writeKv(kvList: Seq[KeyValue]) = {
    val puts = new util.ArrayList[Put](kvList.length)
    kvList.foreach { k =>
      puts.add(new Put(k.getRow).add(k))
    }
    val hTable = hTablePool.getTable(tableName)
    try {
      hTable.put(puts)
      hTable.flushCommits()
    } finally {
      hTable.close()
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
    new KeyValue(row, PointsStorage.PointsFamily, value._1, value._2)
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
                            attrs: Seq[(String, String)] ): Message.Point = {
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

object PointsStorage {
  final val PointsFamily = "t".getBytes(HBaseStorage.Encoding)
}
