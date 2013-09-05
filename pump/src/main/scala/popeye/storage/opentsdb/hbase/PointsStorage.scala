package popeye.storage.opentsdb.hbase

import popeye.transport.proto.Message
import org.apache.hadoop.hbase.client.{Put, HTablePool}
import popeye.storage.opentsdb.hbase.HBaseStorage._
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import java.util
import scala.concurrent.duration.Duration


/**
 * @author Andrey Stepachev
 */
class PointsStorage(tableName: String,
                    hTablePool: HTablePool,
                    metricNames: UniqueId,
                    attributeNames: UniqueId,
                    attributeValues: UniqueId,
                    resolveTimeout: Duration = 15 seconds) {
  val tableBytes = tableName.getBytes(Encoding)

  /**
   * Write points, returned future is awaitable, but can be already completed
   * @param points what to write
   * @return number of written points
   */
  def writePoints(points: Seq[Message.Point])(implicit eCtx: ExecutionContext): Future[Int] = {
    Future.traverse(points)(mkKeyValueFuture).map { kvList =>
      val puts = new util.ArrayList[Put]()
      kvList.foreach { k =>
        puts.add(new Put().add(k))
      }
      val hTable = hTablePool.getTable(tableName)
      try {
        hTable.put(puts)
        hTable.flushCommits()
      } finally {
        hTable.close()
      }
      puts.length
    }.mapTo[Int]
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
        val timestamp: Long = point.getTimestamp
        val baseTime: Int = (timestamp - (timestamp % HBaseStorage.MAX_TIMESPAN)).toInt
        val row = new Array[Byte](metricNames.width + Bytes.SIZEOF_LONG +
          tuple._2.length * (attributeNames.width + attributeValues.width))
        var off = 0
        off = copyBytes(tuple._1, row, off)
        off = copyBytes(Bytes.toBytes(baseTime), row, off)
        for (attr <- tuple._2) {
          off = copyBytes(attr._1, row, off)
          off = copyBytes(attr._2, row, off)
        }

        val qualifiedValue = mkQualifiedValue(point, (timestamp - baseTime).toShort)
        new KeyValue(row, PointsStorage.PointsFamily, qualifiedValue._1, qualifiedValue._2)
    }
  }

  @inline
  private def copyBytes(src: Array[Byte], dst: Array[Byte], off: Int): Int = {
    System.arraycopy(src, 0, dst, off, src.length)
    off + src.length
  }

  /**
   * Produce tuple with qualifier and value represented as byte arrays
   * @param point point to pack
   * @param delta delta from base time
   * @return packed (qualifier, value)
   */
  private def mkQualifiedValue(point: Message.Point, delta: Short): (Array[Byte], Array[Byte]) = {
    if (point.hasFloatValue)
      (Bytes.toBytes(((HBaseStorage.FLAG_FLOAT | 0x3) & (delta << HBaseStorage.FLAG_BITS)).toShort),
        Bytes.toBytes(point.getFloatValue))
    else if (point.hasIntValue)
      (Bytes.toBytes((0x7 & (delta << HBaseStorage.FLAG_BITS)).toShort), Bytes.toBytes(point.getIntValue))
    else
      throw new IllegalArgumentException("Neither int nor float values set on point")

  }
}

object PointsStorage {
  final val PointsFamily = "t".getBytes(HBaseStorage.Encoding)
}
