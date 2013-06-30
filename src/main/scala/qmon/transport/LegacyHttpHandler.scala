package qmon.transport

import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.{ByteString, Timeout}
import akka.actor._
import com.google.protobuf.{ByteString => GoogleByteString}
import spray.can.Http
import spray.can.server.Stats
import spray.util._
import spray.http._
import HttpMethods._
import MediaTypes._
import org.codehaus.jackson.{JsonToken, JsonFactory, JsonParser}
import spray.routing.HttpServiceActor
import qmon.transport.proto.Message.Event


/**
 * @author Andrey Stepachev
 */
class LegacyHttpHandler extends HttpServiceActor with SprayActorLogging {

  implicit val timeout: Timeout = 1.second // for the actor 'asks'

  import context.dispatcher

  // ExecutionContext for the futures and scheduler
  var remains: ByteString = ByteString.empty


  override def preStart() {
    super.preStart()
    remains = ByteString.empty
  }

  def receive = {
    // when a new connection comes in we register ourselves as the connection handler
    case _: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      sender ! index

    case HttpRequest(GET, Uri.Path("/read"), _, _, _) =>
      sender ! index

    case HttpRequest(POST, Uri.Path("/write"), _, entity, _) =>
      for (ev <- Events(entity.buffer)) {
        log.debug(ev.toString)
      }
      sender ! index

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

case class Events(data: Array[Byte]) extends Traversable[Event] {

  def parseValue[U](metric: String, f: (Event) => U, parser: JsonParser) = {
    val event = Event.newBuilder()
    require(parser.getCurrentToken == JsonToken.START_OBJECT)
    while (parser.nextToken != JsonToken.END_OBJECT) {
      require(parser.getCurrentToken == JsonToken.FIELD_NAME)
      parser.nextToken
      parser.getCurrentName match {
        case "type" => {
          require(parser.getText.equalsIgnoreCase("numeric"))
        }
        case "timestamp" => {
          require(parser.getCurrentToken == JsonToken.VALUE_NUMBER_INT)
          event.setTimestamp(parser.getLongValue)
        }
        case "value" => {
          parser.getCurrentToken match {
            case JsonToken.VALUE_NUMBER_INT => {
              event.setIntValue(parser.getLongValue)
            }
            case JsonToken.VALUE_NUMBER_FLOAT => {
              event.setFloatValue(parser.getFloatValue)
            }
            case _ => throw new IllegalArgumentException("Value expected to be float or long")
          }
        }
      }
    }
    event.setMetric(GoogleByteString.copyFromUtf8(metric))
    f(event.build())
    require(parser.getCurrentToken == JsonToken.END_OBJECT)
  }

  def parseMetric[U](f: (Event) => U, parser: JsonParser) = {
    require(parser.getCurrentToken == JsonToken.START_OBJECT)
    parser.nextToken
    val metric = parser.getCurrentName
    parser.nextToken match {
      case JsonToken.START_ARRAY => {
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          parseValue(metric, f, parser)
        }
      }
      case JsonToken.START_OBJECT => parseValue(metric, f, parser)
      case _ => throw new IllegalArgumentException("Object or Array expected, got " + parser.getCurrentToken)
    }
  }

  def parseArray[U](f: (Event) => U, parser: JsonParser) = {
    require(parser.getCurrentToken == JsonToken.START_ARRAY)
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      parseMetric(f, parser)
    }
    parser.nextToken
  }

  def foreach[U](f: (Event) => U) {
    val parser: JsonParser = LegacyHttpHandler.parserFactory.createJsonParser(data)

    parser.nextToken match {
      case JsonToken.START_ARRAY => parseArray(f, parser)
      case JsonToken.START_OBJECT => parseMetric(f, parser)
      case _ => throw new IllegalArgumentException("Object or Array expected, got " + parser.getCurrentToken)
    }
  }
}

object LegacyHttpHandler {
  val parserFactory: JsonFactory = new JsonFactory()
}
