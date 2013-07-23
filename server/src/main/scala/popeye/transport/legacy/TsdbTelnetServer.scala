package popeye.transport.legacy

import akka.actor._
import akka.io._
import akka.pattern.ask
import akka.util.ByteString
import akka.io.IO
import akka.io.TcpPipelineHandler.{WithinActorContext, Init}
import java.net.InetSocketAddress
import net.opentsdb.core.Tags
import popeye.transport.kafka.{ProduceDone, ProducePending}
import popeye.transport.proto.Message.{Tag, Event => PEvent, Batch}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions.asJavaIterable
import popeye.BufferedFSM
import scala.concurrent.duration.FiniteDuration
import popeye.BufferedFSM.{Flush, Todo}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config

class TsdbTelnetHandler(init: Init[WithinActorContext, String, String], kafkaProducer: ActorRef, config: Config)
  extends BufferedFSM[PEvent] with ActorLogging {

  val kafkaTimeout: akka.util.Timeout = new akka.util.Timeout(
    Duration(config.getString("kafka.send.timeout"))
      .asInstanceOf[FiniteDuration])

  override val timeout: FiniteDuration = 1 seconds
  override val flushEntitiesCount: Int = 5000
  private val lastBatchId: AtomicLong = new AtomicLong(0)
  private val pendingCorrelations = new ListBuffer[(ActorRef, Long)]

  sealed case class ReplyOk()

  sealed case class ReplyErr(ex: Throwable)

  override def consumeCollected(todo: Todo[PEvent]) = {
    val me = self
    if (todo.queue.isEmpty) {
      me ! ReplyOk()
    } else {
      log.debug("Flush collected " + todo.entityCnt + " events")
      kafkaProducer.ask(ProducePending(
        Batch.newBuilder().addAllEvent(todo.queue).build,
        0
      ))(kafkaTimeout) onComplete {
        case Success(p@ProduceDone(_, batchId)) =>
          log.debug("ProduceDone: {}", p)
          var pv: Long = 0
          do {
            pv = lastBatchId.get()
          } while (pv < batchId && !lastBatchId.compareAndSet(pv, batchId))
          me ! ReplyOk()
        case Failure(ex: Throwable) =>
          me ! ReplyErr(ex)
        case s =>
          me ! ReplyErr(new IllegalStateException("Unknown event " + s))
      }
    }
  }

  override val handleMessage: TodoFunction = {
    case Event(init.Event(data), todo) =>
      if (data.length > 0) {
        val input = if (data.charAt(data.length - 1) == '\r') {
          data.dropRight(1)
        } else {
          data
        }
        try {
          val strings = Tags.splitString(input, ' ')
          strings(0) match {
            case "put" =>
              todo.copy(entityCnt = todo.entityCnt + 1, queue = todo.queue :+ parsePoint(strings))
            case "commit" =>
              pendingCorrelations += (sender -> strings(1).toLong)
              self ! Flush()
              todo
            case _ =>
              sender ! init.Command("OK" + "\n")
              todo
          }
        } catch {
          case ex: Exception =>
            sender ! init.Command("ERR " + ex.getMessage + "\n")
            context.stop(self)
            todo
        }
      } else {
        todo
      }

    case Event(ReplyOk(), todo) =>
      pendingCorrelations foreach {
        p =>
          p._1 ! init.Command("OK " + p._2 + "=" + lastBatchId.get() + "\n")
      }
      pendingCorrelations.clear()
      todo

    case Event(ReplyErr(ex), todo) =>
      sender ! init.Command("ERR " + ex.getMessage + "\n")
      todo

    case Event(x: Tcp.ConnectionClosed, todo) =>
      log.debug("connection closed")
      context.stop(self)
      todo
  }

  initialize()

  def parsePoint(words: Array[String]): PEvent = {
    val ev = PEvent.newBuilder()
    words(0) = null; // Ditch the "put".
    if (words.length < 5) {
      // Need at least: metric timestamp value tag
      //               ^ 5 and not 4 because words[0] is "put".
      throw new IllegalArgumentException("not enough arguments"
        + " (need least 4, got " + (words.length - 1) + ')');
    }
    ev.setMetric(words(1));
    if (ev.getMetric.isEmpty) {
      throw new IllegalArgumentException("empty metric name");
    }
    ev.setTimestamp(Tags.parseLong(words(2)));
    if (ev.getTimestamp <= 0) {
      throw new IllegalArgumentException("invalid timestamp: " + ev.getTimestamp);
    }
    val value = words(3);
    if (value.length() <= 0) {
      throw new IllegalArgumentException("empty value");
    }
    if (Tags.looksLikeInteger(value)) {
      ev.setIntValue(Tags.parseLong(value));
    } else {
      // floating point value
      ev.setFloatValue(java.lang.Float.parseFloat(value));
    }
    parseTags(ev, 4, words)
    ev.build
  }

  /**
   * Parses tags into a Event.Tag structure.
   * @param tags String array of the form "tag=value".
   * @throws IllegalArgumentException if the tag is malformed.
   * @throws IllegalArgumentException if the tag was already in tags with a
   *                                  different value.
   */
  def parseTags(builder: PEvent.Builder, startIdx: Int, tags: Array[String]) {
    val set = mutable.HashSet[String]()
    for (i <- startIdx to tags.length - 1) {
      val tag = tags(i)
      val kv: Array[String] = Tags.splitString(tag, '=')
      if (kv.length != 2 || kv(0).length <= 0 || kv(1).length <= 0) {
        throw new IllegalArgumentException("invalid tag: " + tag)
      }
      if (!set.add(kv(0))) {
        throw new IllegalArgumentException("duplicate tag: " + tag + ", tags=" + tag)
      }
      builder.addTags(Tag.newBuilder()
        .setName(kv(0))
        .setValue(kv(1)))
    }
  }
}

class TsdbTelnetServer(local: InetSocketAddress, kafka: ActorRef)(implicit val metricRegistry: MetricRegistry) extends Actor with ActorLogging {

  import Tcp._

  implicit def system = context.system

  IO(Tcp) ! Bind(self, local)

  def receive: Receive = {
    case _: Bound ⇒
      context.become(bound(sender))
  }

  def bound(listener: ActorRef): Receive = {
    case Connected(remote, _) ⇒
      val init = TcpPipelineHandler.withLogger(log,
        new StringByteStringAdapter("utf-8") >>
          new DelimiterFraming(maxSize = 2048, delimiter = ByteString('\n'),
            includeDelimiter = false) >>
          new TcpReadWriteAdapter >>
          new BackpressureBuffer(lowBytes = 1 * 1024 * 1024, highBytes = 4 * 1024 * 1024, maxBytes = 10 * 1024 * 1024))

      val connection = sender
      val handler = context.actorOf(Props(new TsdbTelnetHandler(init, kafka, system.settings.config))
        .withDeploy(Deploy.local))
      val pipeline = context.actorOf(TcpPipelineHandler.props(
        init, connection, handler).withDeploy(Deploy.local))

      connection ! Tcp.Register(pipeline)
  }

  override def preStart() {
    super.preStart()
    log.info("Started Tsdb Telnet server")
  }

  override def postStop() {
    super.postStop()
    log.info("Stoped Tsdb Telnet server")
  }
}

object TsdbTelnetServer {
  def start(config: Config, kafkaProducer: ActorRef)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    val hostport = config.getString("tsdb.telnet.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    system.actorOf(Props(new TsdbTelnetServer(addr, kafkaProducer)))
  }
}
