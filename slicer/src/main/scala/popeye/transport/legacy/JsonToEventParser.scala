package popeye.transport.legacy

import popeye.transport.proto.Message.{Attribute, Point}
import org.codehaus.jackson.{JsonToken, JsonParser}
import com.google.protobuf.{ByteString => GoogleByteString}
import akka.actor.{ActorLogging, Actor}
import akka.actor.Status.Failure
import popeye.Logging

class ParserActor extends Actor with ActorLogging {
  def receive = {
    case ParseRequest(data) => {
      try {
        sender ! ParseResult(new JsonToEventParser(data).toList)
      } catch {
        case ex: Throwable => sender ! Failure(ex)
          throw ex
      }
    }
  }
}

sealed class MetricBuilder(string: String) {
  val builder: Point.Builder = {
    val sepIdx: Int = string.indexOf('/')
    require(sepIdx > 1 && sepIdx < string.length, "metric should be in form of 'HOST/metric")
    Point.newBuilder()
      .setMetric("l." + string.substring(sepIdx + 1))
      .addAttributes(Attribute.newBuilder()
      .setName("host")
      .setValue(string.substring(0, sepIdx))
      .build()
    )
  }
}

case class ParseRequest(data: Array[Byte])

case class ParseResult(batch: List[Point])

class JsonToEventParser(data: Array[Byte]) extends Traversable[Point] with Logging {

  def parseValue[U](builder: MetricBuilder, f: (Point) => U, parser: JsonParser) = {
    val event: Point.Builder = builder.builder.clone()
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
    f(event.build())
    require(parser.getCurrentToken == JsonToken.END_OBJECT)
  }

  def parseMetric[U](f: (Point) => U, parser: JsonParser) = {
    require(parser.getCurrentToken == JsonToken.START_OBJECT,
      "Start of OBJECT expected, but " + parser.getCurrentToken + " found")
    parser.nextToken
    val metric = new MetricBuilder(parser.getCurrentName)
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

  def parseArray[U](f: (Point) => U, parser: JsonParser) = {
    require(parser.getCurrentToken == JsonToken.START_ARRAY)
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      parseMetric(f, parser)
      parser.nextToken
    }
    parser.nextToken
  }

  def foreach[U](f: (Point) => U) {
    val parser: JsonParser = LegacyHttpHandler.parserFactory.createJsonParser(data)

    parser.nextToken match {
      case JsonToken.START_ARRAY => parseArray(f, parser)
      case JsonToken.START_OBJECT => parseMetric(f, parser)
      case _ => throw new IllegalArgumentException("Object or Array expected, got " + parser.getCurrentToken)
    }
  }
}
