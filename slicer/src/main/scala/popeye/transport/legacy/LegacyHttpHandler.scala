package popeye.transport.legacy

import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import akka.actor._
import com.google.protobuf.{ByteString => GoogleByteString}
import spray.can.Http
import spray.util._
import spray.http._
import HttpMethods._
import MediaTypes._
import org.codehaus.jackson.{JsonParseException, JsonFactory}
import popeye.transport.proto.Message.{Point, Batch}
import akka.actor.SupervisorStrategy.{Restart, Stop}
import scala.collection.JavaConversions.asJavaIterable
import java.net.InetSocketAddress
import akka.io.IO
import spray.http.HttpRequest
import scala.util.Failure
import popeye.transport.kafka.ProduceDone
import akka.actor.OneForOneStrategy
import spray.http.HttpResponse
import scala.util.Success
import popeye.transport.kafka.ProducePending
import com.typesafe.config.Config
import popeye.Instrumented
import com.codahale.metrics.MetricRegistry
import popeye.transport.proto.PackedPoints
import scala.concurrent.Promise


class LegacyHttpHandlerMetrics (override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer("write")
  val readTimer = metrics.timer("read")
  val requestsBatches = metrics.histogram("batches", "size")
  val totalBatches = metrics.meter("batches", "total")
  val failedBatches = metrics.meter("batches", "failed")
}

/**
 * @author Andrey Stepachev
 */
class LegacyHttpHandler(config: Config, kafkaProducer: ActorRef, metrics: LegacyHttpHandlerMetrics) extends Actor with SprayActorLogging {

  implicit val timeout: Timeout = 5.second
  // for the actor 'asks'
  val kafkaTimeout: Timeout = new Timeout(config.getDuration("legacy.http.produce.timeout").asInstanceOf[FiniteDuration])

  import context.dispatcher


  override val supervisorStrategy = {
    OneForOneStrategy()({
      case _: Exception => Restart
    })
  }

  def receive = {
    case x: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      sender ! index

    case HttpRequest(GET, Uri.Path("/read"), _, _, _) =>
      metrics.readTimer.time(sender ! index)

    case HttpRequest(POST, Uri.Path("/write"), _, entity, _) =>
      val timer = metrics.writeTimer.timerContext()
      val client = sender
      val parser: ActorRef = context.actorOf(Props[ParserActor])

      val result = for {
        parsed <- ask(parser, ParseRequest(entity.buffer)).mapTo[ParseResult]
        stored <- ask(kafkaProducer, ProducePending(None)(PackedPoints(parsed.batch)))(kafkaTimeout)
      } yield {
        metrics.requestsBatches.update(parsed.batch.size)
        timer.stop()
        stored
      }
      result onComplete {
        case Success(r: ProduceDone) =>
          client ! HttpResponse(200, "Ok: " + r.correlationId + ":" + r.assignedBatchId)
          timer.stop()
        case Success(x) =>
          client ! HttpResponse(status = StatusCodes.InternalServerError, entity = "unknown message " + x)
        case Failure(ex: JsonParseException) =>
          client ! HttpResponse(status = StatusCodes.UnprocessableEntity, entity = ex.getMessage)
        case Failure(ex) =>
          log.error(ex, "Failed")
          client ! HttpResponse(status = StatusCodes.InternalServerError, entity = ex.getMessage)
      }

    case _: HttpRequest => sender ! HttpResponse(status = 404, entity = "Unknown resource!")
  }

  def eventsPresentation(events: Traversable[Point]) = {
    val eventList = events.map({
      _.toString
    }).mkString("<br/>")
    HttpResponse(
      entity = HttpEntity(`text/html`,
        <html>
          <body>
            <h1>Got events</h1>{eventList}
          </body>
        </html>.toString()
      )
    )
  }


  lazy val index = HttpResponse(
    entity = HttpEntity(`text/html`,
      <html>
        <body>
          <h1>Say hello to
            <i>Popeye</i>
            !</h1>
          <ul>
            <li>
              <a href="/read">Read points</a>
            </li>
            <li>
              <a href="/write">Write points</a>
            </li>
            <li>
              <a href="/q">Query</a>
            </li>
          </ul>
        </body>
      </html>.toString()
    )
  )
}

object LegacyHttpHandler {
  implicit val timeout: Timeout = 5 seconds

  def start(config: Config, kafkaProducer: ActorRef)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    val handler = system.actorOf(
      Props(new LegacyHttpHandler(config, kafkaProducer, new LegacyHttpHandlerMetrics(metricRegistry))),
      name = "legacy-http")
    val hostport = config.getString("legacy.http.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    IO(Http) ? Http.Bind(
      listener = handler,
      endpoint = addr,
      backlog = config.getInt("legacy.http.backlog"),
      options = Nil,
      settings = None)
    handler
  }

}
