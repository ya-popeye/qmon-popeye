package popeye.transport.server.http

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.codahale.metrics.MetricRegistry
import com.google.protobuf.{ByteString => GoogleByteString}
import com.typesafe.config.Config
import java.net.InetSocketAddress
import org.codehaus.jackson.JsonParseException
import popeye.pipeline.DispatcherProtocol.{Done, Pending}
import popeye.proto.Message.Point
import popeye.proto.PackedPoints
import popeye.{Logging, Instrumented}
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import spray.can.Http
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import spray.http._
import spray.util._


/**
 * @author Andrey Stepachev
 */
class HttpPointsServer(config: Config, kafkaProducer: ActorRef, metrics: HttpPointsServerMetrics) extends Actor with Logging {

  implicit val timeout: Timeout = 5.second
  // for the actor 'asks'
  val kafkaTimeout: Timeout = new Timeout(config.getDuration("server.http.produce.timeout").asInstanceOf[FiniteDuration])

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
        parsed <- ask(parser, ParseRequest(entity.data.toByteString)).mapTo[ParseResult]
        stored <- ask(kafkaProducer, Pending(None)(PackedPoints(parsed.batch)))(kafkaTimeout)
      } yield {
        metrics.requestsBatches.update(parsed.batch.size)
        timer.stop()
        stored
      }
      result onComplete {
        case Success(r: Done) =>
          client ! HttpResponse(200, "Ok: " + r.correlationId + ":" + r.assignedBatchId)
          timer.stop()
        case Success(x) =>
          client ! HttpResponse(status = StatusCodes.InternalServerError, entity = "unknown message " + x)
        case Failure(ex: JsonParseException) =>
          client ! HttpResponse(status = StatusCodes.UnprocessableEntity, entity = ex.getMessage)
        case Failure(ex) =>
          error("Failed", ex)
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

object HttpPointsServer {
  implicit val timeout: Timeout = 5 seconds

  def start(config: Config, kafkaProducer: ActorRef)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    val handler = system.actorOf(
      Props.apply(new HttpPointsServer(config, kafkaProducer, new HttpPointsServerMetrics(metricRegistry))),
      name = "server-http")
    val hostport = config.getString("server.http.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    IO(Http) ? Http.Bind(
      listener = handler,
      endpoint = addr,
      backlog = config.getInt("server.http.backlog"),
      options = Nil,
      settings = None)
    handler
  }

}
