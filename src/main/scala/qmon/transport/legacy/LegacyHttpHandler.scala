package qmon.transport.legacy

import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import akka.actor._
import com.google.protobuf.{ByteString => GoogleByteString}
import spray.can.Http
import spray.can.server.Stats
import spray.util._
import spray.http._
import HttpMethods._
import MediaTypes._
import org.codehaus.jackson.{JsonParseException, JsonFactory}
import qmon.transport.proto.Message.{Event, Batch}
import akka.actor.SupervisorStrategy.{Stop, Escalate}
import scala.util.{Failure, Success}


/**
 * @author Andrey Stepachev
 */
class LegacyHttpHandler extends Actor with SprayActorLogging {

  implicit val timeout: Timeout = 1.second // for the actor 'asks'

  import context.dispatcher

  override val supervisorStrategy = {
    OneForOneStrategy()({
      case _: Exception => Stop
    })
  }


  def receive = {
    // when a new connection comes in we register ourselves as the connection handler
    case _: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      sender ! index

    case HttpRequest(GET, Uri.Path("/read"), _, _, _) =>
      sender ! index

    case HttpRequest(POST, Uri.Path("/write"), _, entity, _) =>
      val client = sender
      val parser: ActorRef = context.actorOf(Props[ParserActor])
      val future = parser ? ParseRequest(entity.buffer)
      future.onComplete({
        case Success(batch) => client ! HttpResponse(status = 200, entity = batch.toString)
        case Failure(ex: JsonParseException) =>
          client ! HttpResponse(status = StatusCodes.UnprocessableEntity, entity = ex.getMessage)
        case Failure(ex) =>
          client ! HttpResponse(status = StatusCodes.InternalServerError, entity = ex.getStackTraceString)
      })

    case HttpRequest(GET, Uri.Path("/server-stats"), _, _, _) =>
      val client = sender
      context.actorFor("/user/IO-HTTP/listener-0") ? Http.GetStats onSuccess {
        case x: Stats => client ! statsPresentation(x)
      }

    case _: HttpRequest => sender ! HttpResponse(status = 404, entity = "Unknown resource!")

  }


  def statsPresentation(s: Stats) = HttpResponse(
    entity = HttpEntity(`text/html`,
      <html>
        <body>
          <h1>HttpServer Stats</h1>
          <table>
            <tr>
              <td>uptime:</td> <td>
              {s.uptime.formatHMS}
            </td>
            </tr>
            <tr>
              <td>totalRequests:</td> <td>
              {s.totalRequests}
            </td>
            </tr>
            <tr>
              <td>openRequests:</td> <td>
              {s.openRequests}
            </td>
            </tr>
            <tr>
              <td>maxOpenRequests:</td> <td>
              {s.maxOpenRequests}
            </td>
            </tr>
            <tr>
              <td>totalConnections:</td> <td>
              {s.totalConnections}
            </td>
            </tr>
            <tr>
              <td>openConnections:</td> <td>
              {s.openConnections}
            </td>
            </tr>
            <tr>
              <td>maxOpenConnections:</td> <td>
              {s.maxOpenConnections}
            </td>
            </tr>
            <tr>
              <td>requestTimeouts:</td> <td>
              {s.requestTimeouts}
            </td>
            </tr>
          </table>
        </body>
      </html>.toString()
    )
  )

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
            <li>
              <a href="/server-stats">/server-stats</a>
            </li>
          </ul>
        </body>
      </html>.toString()
    )
  )
}


object LegacyHttpHandler {
  val parserFactory: JsonFactory = new JsonFactory()
}
