package popeye.storage.opentsdb

import popeye.{Instrumented, Logging}
import popeye.ConfigUtil._
import akka.actor._
import com.typesafe.config.Config
import kafka.consumer._
import java.util.Properties
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.Restart
import scala.Option
import scala.Some
import akka.actor.OneForOneStrategy
import com.codahale.metrics.MetricRegistry
import scala.collection.mutable.ArrayBuffer
import popeye.transport.proto.{PackedPoints, Message}
import popeye.transport.proto.Message.Point
import net.opentsdb.core.{TSDB, EventPersistFuture}
import org.hbase.async.HBaseClient
import TsdbPointConsumerProtocol._
import akka.routing.FromConfig
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import com.google.protobuf.InvalidProtocolBufferException

/**
 * @author Andrey Stepachev
 */

class ConsumerInitializationException extends Exception

class BatchProcessingFailedException extends Exception

case class TsdbPointConsumerMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val consumeTimer = metrics.timer("tsdb.consume.time")
  val pointsMeter = metrics.meter("tsdb.consume.points")
  val batchSizeHist = metrics.histogram("tsdb.consume.batch.size")
  val batchCompleteHist = metrics.meter("tsdb.consume.batch.complete")
  val batchFailedHist = metrics.meter("tsdb.consume.batch.complete")
  val batchDecodeFailuresMeter = metrics.meter("tsdb.consume.bached.decode-errors")
  val writeTimer = metrics.timer("tsdb.write.write")
  val writeBatchSizeHist = metrics.histogram("tsdb.write.batch-size.write")
  val incomingBatchSizeHist = metrics.histogram("tsdb.write.batch-size.incoming")
}

object TsdbPointConsumerProtocol {

  sealed class ConsumeCommand

  case object CheckAvailable

  case class ConsumeDone(batches: Traversable[Long]) extends ConsumeCommand

  case class ConsumeFailed(batches: Traversable[Long], cause: Throwable) extends ConsumeCommand

}

class TsdbPointConsumer(config: Config, tsdb: TSDB, val metrics: TsdbPointConsumerMetrics)
  extends Actor with ActorLogging {

  import TsdbPointConsumer._

  val topic = config.getString("kafka.points.topic")
  val group = config.getString("tsdb.consume.group")

  val pair: ConsumerPair = TsdbPointConsumer.createConsumer(topic, consumerConfig(config))
  if (pair.consumer.isEmpty)
    throw new ConsumerInitializationException
  val consumer = pair.consumer
  val connector = pair.connector
  lazy val maxBatchSize = config.getLong("tsdb.consume.batch-size")
  lazy val checkTick = toFiniteDuration(config.getMilliseconds("tsdb.consume.check-tick"))

  var checker: Option[Cancellable] = None

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _ ⇒ Restart
    }

  override def preStart() {
    super.preStart()
    // jitter to prevent rebalance deadlock
    //context.system.scheduler.scheduleOnce(Random.nextInt(10) seconds, self, ConsumeDone(Nil))
    log.debug("Starting TsdbPointConsumer for group " + group + " and topic " + topic)
    import context.dispatcher
    checker = Some(context.system.scheduler.schedule(checkTick, checkTick, self, CheckAvailable))
  }

  override def postStop() {
    checker foreach { _.cancel() }
    super.postStop()
    log.debug("Stopping TsdbPointConsumer for group " + group + " and topic " + topic)
    connector.shutdown()
  }

  def receive = {
    case CheckAvailable =>
      doNext()

    case ConsumeDone(batches) =>
      connector.commitOffsets
      metrics.batchCompleteHist.mark()
      doNext()

    case ConsumeFailed(batches, ex) =>
      log.error(ex, "Batches {} failed", batches.size)
      metrics.batchFailedHist.mark()
      throw new BatchProcessingFailedException
  }

  def doNext() = {
    val batchIds = new ArrayBuffer[Long]
    val batch = new ArrayBuffer[Message.Point]
    val tctx = metrics.consumeTimer.timerContext()
    val iterator = consumer.get.iterator()
    try {
      while (iterator.hasNext && batch.size < maxBatchSize) {
        val msg: MessageAndMetadata[Array[Byte], Array[Byte]] = iterator.next()
        try {
          val (batchId, points) = PackedPoints.decodeWithBatchId(msg.message)
          metrics.batchSizeHist.update(points.size)
          batchIds += batchId
          batch ++= points
        } catch {
          case e: InvalidProtocolBufferException =>
            metrics.batchDecodeFailuresMeter.mark()
        }
      }
    } catch {
      case ex: ConsumerTimeoutException => // ok
      case ex: Throwable =>
        log.error("Failed to consume", ex)
        throw ex
    }
    if (batchIds.size > 0) {
      metrics.pointsMeter.mark(batch.size)
      tctx.close()
      sendBatch(batchIds, batch)
    }
  }

  def sendBatch(batches: Seq[Long], events: Seq[Point]) = {
    val ctx = metrics.writeTimer.timerContext()
    metrics.writeBatchSizeHist.update(events.size)
    new EventPersistFuture(tsdb, events.toArray) {
      protected def complete() {
        val nanos = ctx.stop()
        self ! ConsumeDone(batches)
        if (log.isDebugEnabled)
          log.debug("Processing of batches {} complete in {}ms", batches.size, NANOSECONDS.toMillis(nanos))
      }

      protected def fail(cause: Throwable) {
        val nanos = ctx.stop()
        self ! ConsumeFailed(batches, cause)
        log.error(cause, "Processing of batches {} failed in {}ms", batches.size, NANOSECONDS.toMillis(nanos))
      }
    }
  }

}

object TsdbPointConsumer extends Logging {

  def props(config: Config, hbaseClient: Option[HBaseClient] = None)(implicit system: ActorSystem, metricRegistry: MetricRegistry) = {
    val hbc = hbaseClient getOrElse {
      val cluster: String = config.getString("tsdb.zk.cluster")
      val zkPath: String = config.getString("tsdb.zk.path")
      log.info(s"Creating HBaseClient: ${cluster}/${zkPath}")
      new HBaseClient(cluster, zkPath)
    }
    val tsdb: TSDB = new TSDB(hbc,
      config.getString("tsdb.table.series"),
      config.getString("tsdb.table.uids"))
    system.registerOnTermination(tsdb.shutdown())
    system.registerOnTermination(hbc.shutdown())

    val metrics = TsdbPointConsumerMetrics(metricRegistry)
    Props(
      new TsdbPointConsumer(
        config,
        tsdb,
        metrics)
    )
  }

  def start(config: Config, hbaseClient: Option[HBaseClient] = None)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    system.actorOf(props(config, hbaseClient)
      .withRouter(FromConfig())
      .withDispatcher("tsdb.consume.dispatcher"), "tsdb-writer")
  }

  def consumerConfig(globalConfig: Config): ConsumerConfig = {
    val config: Config = globalConfig.getConfig("tsdb.consumer")
    val consumerProps: Properties = config
    val timeout = globalConfig.getMilliseconds("tsdb.consumer.timeout")
    consumerProps.put("consumer.timeout.ms", timeout.toString)
    consumerProps.put("group.id", globalConfig.getString("tsdb.consume.group"))
    new ConsumerConfig(consumerProps)
  }

  class ConsumerPair(val connector: ConsumerConnector, val consumer: Option[KafkaStream[Array[Byte], Array[Byte]]]) {
    def shutdown = {
      connector.shutdown()
    }
  }

  def createConsumer(topic: String, config: ConsumerConfig): ConsumerPair = {
    val consumerConnector: ConsumerConnector = Consumer.create(config)

    val topicThreadCount = Map((topic, 1))
    val topicMessageStreams = consumerConnector.createMessageStreams(topicThreadCount)
    val streams = topicMessageStreams.get(topic)

    val stream = streams match {
      case Some(List(s)) => Some(s)
      case _ => {
        log.error("Did not get a valid stream from topic " + topic)
        None
      }
    }
    log.info(s"Consumer created")
    new ConsumerPair(consumerConnector, stream)
  }
}




