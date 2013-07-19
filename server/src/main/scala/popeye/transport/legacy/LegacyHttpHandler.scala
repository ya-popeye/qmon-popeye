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
import popeye.transport.proto.Message.{Event, Batch}
import akka.actor.SupervisorStrategy.Stop
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


/**
 * @author Andrey Stepachev
 */
class LegacyHttpHandler(kafkaProducer: ActorRef) extends Actor with SprayActorLogging {

  implicit val timeout: Timeout = 5.second
  // for the actor 'asks'
  val kafkaTimeout: Timeout = new Timeout(context.system.settings.config
    .getDuration("kafka.send.timeout").asInstanceOf[FiniteDuration])

  import context.dispatcher

  override val supervisorStrategy = {
    OneForOneStrategy()({
      case _: Exception => Stop
    })
  }


  def receive = {
    case x: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      sender ! index

    case HttpRequest(GET, Uri.Path("/read"), _, _, _) =>
      sender ! index

    case HttpRequest(POST, Uri.Path("/write"), _, entity, _) =>
      val client = sender
      val parser: ActorRef = context.actorOf(Props[ParserActor])

      val result = for {
        parsed <- ask(parser, ParseRequest(entity.buffer)).mapTo[ParseResult]
        stored <- ask(kafkaProducer, ProducePending(
          Batch.newBuilder().addAllEvent(parsed.batch).build,
          0
        ))(kafkaTimeout)
      } yield {
        stored
      }
      result onComplete {
        case Success(r: ProduceDone) =>
          client ! HttpResponse(200, "Ok: " + r.correlationId + ":" + r.assignedBatchId)
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

  def eventsPresentation(events: Traversable[Event]) = {
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
  val parserFactory: JsonFactory = new JsonFactory()
  implicit val timeout: Timeout = 5 seconds

  def bind(config: Config, kafkaProducer: ActorRef)(implicit system: ActorSystem): ActorRef = {
    val handler = system.actorOf(Props(new LegacyHttpHandler(kafkaProducer)), name = "legacy-http")
    val hostport = config.getString("http.legacy.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    IO(Http) ? Http.Bind(
      listener = handler,
      endpoint = addr,
      backlog = config.getInt("http.legacy.backlog"),
      options = Nil,
      settings = None)
    handler
  }

}
