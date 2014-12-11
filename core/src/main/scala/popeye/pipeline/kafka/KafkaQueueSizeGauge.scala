package popeye.pipeline.kafka

import java.util.concurrent.atomic.AtomicReference

import akka.actor.Scheduler
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import popeye.Logging
import popeye.util.{ZkConnect, KafkaMetaRequests}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Random, Failure, Try}

class KafkaQueueSizeGauge(zkConnect: ZkConnect,
                          brokers: Seq[(String, Int)],
                          consumerGroupId: String,
                          topic: String) extends Logging {

  private val queueSizes = new AtomicReference[Try[Map[Int, Long]]](
    Failure(new RuntimeException("queue size was not fetched yet"))
  )

  def getTotalQueueSizeOption = queueSizes.get.toOption.map(_.values.sum)

  def getMaxQueueSizeOption = queueSizes.get.toOption.map(_.values.max)

  def start(pollPeriod: FiniteDuration, scheduler: Scheduler)(implicit ectx: ExecutionContext) = {
    val random = new Random()
    val randomDuration = FiniteDuration(random.nextInt(pollPeriod.toMillis.toInt), MILLISECONDS)
    scheduler.schedule(initialDelay = randomDuration, interval = pollPeriod) {
      val newQueueSizes = Try(fetchQueueSizes)
      newQueueSizes match {
        case Failure(t) => error("kafka queue size fetch failed", t)
        case _ => ()
      }
      queueSizes.set(newQueueSizes)
    }
  }

  def fetchQueueSizes: Map[Int, Long] = {
    val latestOffsets = new KafkaMetaRequests(brokers, topic).fetchLatestOffsets()
    info(f"latest offsets: ${ prettyPrintMap(latestOffsets) }")
    val consumerOffsets = fetchConsumerOffsets
    info(f"consumer offsets: ${ prettyPrintMap(consumerOffsets) }")
    val diff = merge(latestOffsets, consumerOffsets)(_ - _)
    info(f"kafka queue sizes: ${ prettyPrintMap(diff) }")
    diff
  }

  private def merge[A, B, C](left: Map[A, B], right: Map[A, B])(f: (B, B) => C) = {
    val allKeys = (left.keys ++ right.keys).toSet
    allKeys.map {
      key => (key, f(left(key), right(key)))
    }.toMap
  }

  private def fetchConsumerOffsets: Map[Int, Long] = {
    val offsetsDirPath = f"/consumers/$consumerGroupId/offsets/$topic"
    withZkClient {
      zk =>
        val offsetPaths = zk.getChildren(offsetsDirPath).asScala
        offsetPaths.map {
          offsetPath =>
            val offsetString: String = zk.readData(f"$offsetsDirPath/$offsetPath")
            val partition = offsetPath.toInt
            val offset = offsetString.toLong
            (partition, offset)
        }.toMap
    }
  }

  private def prettyPrintMap[A: Ordering, B](mapToPrint: Map[A, B]) = {
    mapToPrint.toList.sortBy(_._1).map { case (key, value) => f"$key -> $value" }.mkString("(", ", ", ")")
  }

  private def withZkClient[T](operation: ZkClient => T): T = {

    val zkClient = new ZkClient(zkConnect.toZkConnectString, 5000, 5000, ZKStringSerializer)
    try {
      operation(zkClient)
    } finally {
      zkClient.close()
    }
  }

}
