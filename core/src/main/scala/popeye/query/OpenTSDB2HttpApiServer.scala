package popeye.query

import com.codahale.metrics.MetricRegistry
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.{ObjectNode, JsonNodeFactory}
import popeye.storage.hbase.HBaseStorage._
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition.{SingleValueName, MultipleValueNames, AllValueNames}

import scala.collection.JavaConverters._
import akka.actor.{Props, ActorRef, ActorSystem, Actor}
import org.codehaus.jackson.map.ObjectMapper
import popeye.{PointRope, Instrumented, Logging}
import spray.can.Http
import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.http._
import spray.http.MediaTypes.`application/json`
import popeye.query.OpenTSDB2HttpApiServer._

import scala.concurrent.{Promise, Future, ExecutionContext}

class OpenTSDB2HttpApiServerHandler(storage: PointsStorage,
                                    executionContext: ExecutionContext,
                                    metrics: OpenTSDB2HttpApiServerMetrics) extends Actor with Logging {

  val storageRequestCancellation = Promise[Nothing]()
  implicit val exct = executionContext

  override def receive: Receive = {
    case request@HttpRequest(GET, path@Uri.Path("/api/suggest"), _, _, _) =>
      def parameter(name: String, errorMsg: => String) = path.query.get(name).toRight(errorMsg)
      val maxSuggestions = path.query.get("max").map(_.toInt).getOrElse(25)
      val suggestionsFutureOrErrorMsg =
        for {
          suggestKey <- parameter("type", "suggest type (type) is not set").right
          suggestType <- suggestTypes.get(suggestKey)
            .toRight(f"no such suggest type: $suggestKey, try one of ${ suggestTypes.keys }").right
          namePrefix <- parameter("q", "name prefix (q) is not set").right
        } yield Future {
          val suggestions: Seq[String] = storage.getSuggestions(namePrefix, suggestType, maxSuggestions)
          suggestions.map(s => "\"" + s + "\"").mkString("[", ", ", "]")
        }
      val client = sender
      suggestionsFutureOrErrorMsg.fold(
        errorMsg => client ! HttpResponse(status = StatusCodes.BadRequest, entity = HttpEntity(errorMsg)),
        responseFuture => responseFuture.map {
          json =>
            val responseEntity = HttpEntity(ContentType(`application/json`), json)
            client ! HttpResponse(entity = responseEntity, headers = List(`Access-Control-Allow-Origin`(AllOrigins)))
            info(f"suggest request served: $request)")
            info(json)
        }.onFailure {
          case t: Throwable =>
            client ! HttpResponse(status = StatusCodes.InternalServerError)
            info(f"suggest query failed", t)
        }
      )
    case request@HttpRequest(OPTIONS, path@Uri.Path("/api/query"), _, _, _) =>
      val headers = List(
        `Access-Control-Allow-Origin`(AllOrigins),
        `Access-Control-Allow-Methods`(OPTIONS, POST),
        `Access-Control-Allow-Headers`("Content-Type")
      )
      sender ! HttpResponse(headers = headers)

    case request@HttpRequest(POST, path@Uri.Path("/api/query"), _, _, _) =>
      val json = objectMapper.readTree(request.entity.asString)
      info(s"points request: ${ objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json) }")
      val startMillis = json.get("start").getLongValue
      val stopMillis = if (json.has("end")) json.get("end").getLongValue else System.currentTimeMillis()
      val queries = json.get("queries").asScala.toList.map(parseTsQuery)
      val totalTimeContext = metrics.totalReadTime.timerContext()
      val queryResults = Future.traverse(queries) {
        query =>
          val storageReadTimeContext = metrics.storageReadTime.timerContext()
          val pointGroupsFuture = storage.getPoints(
            query.metricName,
            ((startMillis / 1000).toInt, (stopMillis / 1000).toInt),
            query.tags,
            storageRequestCancellation.future
          )
          val aggregator = aggregators(query.aggregatorKey)
          val downsampleOption = query.downsample.map {
            case (interval, aggrgKey) => (interval, aggregators(aggrgKey))
          }
          for {
            pointGroups <- pointGroupsFuture
          } yield {
            storageReadTimeContext.stop()
            debug(s"got point groups sizes: ${ pointGroups.groupsMap.mapValues(_.mapValues(_.size)) }")
            val aggregatedPoints = metrics.aggregationTime.time {
              aggregatePoints(pointGroups, aggregator, query.isRate, downsampleOption)
            }
            (query.metricName, aggregatedPoints)
          }
      }
      val responseStringFuture = queryResults.map {
        results =>
          val resultObjs = results.map { case (metricName, groups) =>
            groups.map { case (tags, points) => pointGroupToJsonObj(metricName, tags, points) }
          }.flatten
          val resultsArray = JsonNodeFactory.instance.arrayNode()
          for (obj <- resultObjs) {
            resultsArray.add(obj)
          }
          objectMapper.writeValueAsString(resultsArray)
      }
      val headers = List(
        `Access-Control-Allow-Origin`(AllOrigins),
        `Access-Control-Allow-Headers`("Content-Type")
      )
      val client = sender
      responseStringFuture.map {
        response =>
          client ! HttpResponse(headers = headers, entity = HttpEntity(response))
          totalTimeContext.stop()
      }.onFailure {
        case e: Exception =>
          log.error("request failed", e)
          client ! HttpResponse(status = StatusCodes.InternalServerError, entity = HttpEntity(e.getMessage))
      }

    case request: HttpRequest =>
      log.debug(s"bad request: $request")
      sender ! HttpResponse(status = StatusCodes.BadRequest, entity = HttpEntity("not implemented"))

    case msg: Http.ConnectionClosed =>
      info(f"closing connection on message: $msg")
      storageRequestCancellation.tryFailure(
        new RuntimeException("http connection was closed; storage request cancelled")
      )
    case msg => println(msg)
  }

  private def parseTsQuery(jsonQuery: JsonNode) = {
    val tags =
      if (jsonQuery.has("tags")) {
        jsonQuery.get("tags").getFields.asScala.toList.map {
          jsonTag =>
            val key = jsonTag.getKey
            val value = parseValueFilterCondition(jsonTag.getValue.getTextValue)
            (key, value)
        }.toMap
      } else {
        Map[String, ValueNameFilterCondition]()
      }
    val downsample =
      if (jsonQuery.has("downsample")) {
        val tokens = jsonQuery.get("downsample").getTextValue.split("-")
        require(tokens.length == 2, "bad downsample arg format")
        val Array(duration, aggregatorKey) = tokens
        Some((parseDownsampleDuration(duration), aggregatorKey))
      } else {
        None
      }
    TimeSeriesQuery(
      jsonQuery.get("metric").getTextValue,
      jsonQuery.get("aggregator").getTextValue,
      isRate = jsonQuery.has("rate") && jsonQuery.get("rate").asBoolean(),
      downsample,
      tags
    )
  }

  private def pointGroupToJsonObj(metricName: String,
                                  tags: PointAttributes,
                                  points: Seq[(Int, Double)]): ObjectNode = {

    val jsonFactory = JsonNodeFactory.instance
    val resultNode = jsonFactory.objectNode()
    resultNode.put("metric", metricName)
    val tagObj = jsonFactory.objectNode()
    for ((key, value) <- tags) {
      tagObj.put(key, value)
    }
    resultNode.put("tags", tagObj)
    val dpsObj = jsonFactory.objectNode()
    for ((ts, value) <- points) {
      dpsObj.put(ts.toString, value)
    }
    resultNode.put("dps", dpsObj)
    resultNode
  }

  private def parseValueFilterCondition(valueString: String) = {
    if (valueString == "*") {
      AllValueNames
    } else if (valueString.contains("|")) {
      MultipleValueNames(valueString.split("\\|").toList)
    } else {
      SingleValueName(valueString)
    }
  }

  private def parseDownsampleDuration(duration: String) = {
    val number = duration.init.toInt
    val multiplierKey = duration.last
    val multiplier = multiplierKey match {
      case 's' => 1 // seconds
      case 'm' => 60; // minutes
      case 'h' => 3600; // hours
      case 'd' => 3600 * 24; // days
      case 'w' => 3600 * 24 * 7; // weeks
      case 'y' => 3600 * 24 * 365; // years (no leap years)
    }
    number * multiplier
  }
}

class OpenTSDB2HttpApiServerMetrics(name: String, override val metricRegistry: MetricRegistry) extends Instrumented {
  val totalReadTime = metrics.timer(f"$name.total.read.time")
  val storageReadTime = metrics.timer(f"$name.storage.read.time")
  val aggregationTime = metrics.timer(f"$name.aggregation.time")
}

object OpenTSDB2HttpApiServer {

  import PointsStorage.NameType._

  case class TimeSeriesQuery(metricName: String,
                             aggregatorKey: String,
                             isRate: Boolean,
                             downsample: Option[(Int, String)],
                             tags: Map[String, ValueNameFilterCondition])

  val objectMapper = new ObjectMapper()

  val suggestTypes = Map(
    "metrics" -> MetricType,
    "tagk" -> AttributeNameType,
    "tagv" -> AttributeValueType
  )


  val aggregators = Map[String, Seq[Double] => Double](
    "sum" -> (seq => seq.sum),
    "min" -> (seq => seq.min),
    "max" -> (seq => seq.max),
    "avg" -> (seq => seq.sum / seq.size)
  )

  def aggregatePoints(pointsGroups: PointsGroups,
                      interpolationAggregator: Seq[Double] => Double,
                      rate: Boolean,
                      downsamplingOption: Option[(Int, Seq[Double] => Double)]): Map[PointAttributes, Seq[(Int, Double)]] = {
    def toGraphPointIterator(points: PointRope) = {
      val graphPoints = points.iterator.map {
        point => (point.timestamp, point.value)
      }
      val downsampled = downsamplingOption.map {
        case (interval, aggregator) => PointSeriesUtils.downsample(graphPoints, interval, aggregator)
      }.getOrElse(graphPoints)
      if (rate) {
        PointSeriesUtils.differentiate(downsampled)
      } else {
        downsampled
      }
    }
    pointsGroups.groupsMap.mapValues {
      group =>
        val graphPointIterators = group.values.map(toGraphPointIterator).toSeq
        val aggregated = PointSeriesUtils.interpolateAndAggregate(graphPointIterators, interpolationAggregator)
        aggregated.toList
    }
  }
}

class OpenTSDB2HttpApiServer(metrics: OpenTSDB2HttpApiServerMetrics) extends HttpServerFactory {


  override def createHandler(system: ActorSystem,
                             storage: PointsStorage,
                             executionContext: ExecutionContext): ActorRef = {
    system.actorOf(Props(new OpenTSDB2HttpApiServerHandler(storage, executionContext, metrics)))
  }


}
