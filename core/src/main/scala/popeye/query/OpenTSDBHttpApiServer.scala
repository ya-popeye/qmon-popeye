package popeye.query

import akka.actor.{ActorSystem, Props, Actor}
import popeye.Logging
import akka.pattern.ask
import java.net.InetSocketAddress
import akka.io.IO
import spray.can.Http
import com.typesafe.config.Config
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import spray.http._
import spray.http.HttpMethods._
import spray.can.server.ServerSettings
import popeye.query.OpenTSDBHttpApiServer._
import java.text.SimpleDateFormat
import popeye.storage.hbase.HBaseStorage._
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition._
import spray.http.HttpResponse
import spray.http.HttpRequest


class OpenTSDBHttpApiServer(storage: PointsStorage, executionContext: ExecutionContext) extends Actor with Logging {

  val query = """/q&.*""".r
  implicit val eCtx = executionContext

  def receive: Actor.Receive = {
    case x: Http.Connected => sender ! Http.Register(self)
    case request@HttpRequest(GET, path@Uri.Path("/q"), _, _, _) =>
      info(f"request: $request)")
      val savedClient = sender
      val parameters = queryStringToMap(path.query.value)
      def parameter(name: String, errorMsg: => String) = parameters.get(name).toRight(errorMsg)

      val resultFutureOrErrMessage =
        for {
          startDate <- parameter("start", "start is not set").right
          endDate <- parameter("end", "end is not set").right
          metricQueryString <- parameter("m", "metric is not set").right
          timeSeriesQuery <- parseTimeSeriesQuery(metricQueryString).right
          aggregator <- aggregatorsMap.get(timeSeriesQuery.aggregatorKey)
            .toRight(f"no such aggregation: $timeSeriesQuery; available aggregations: ${aggregatorsMap.keys}").right
        } yield {
          val startTime = parseTime(startDate)
          val endTime = parseTime(endDate)
          storage.getPoints(timeSeriesQuery.metricName, (startTime, endTime), timeSeriesQuery.tags)
            .flatMap(_.toFuturePointsGroups)
            .map(pointsGroups => aggregatePoints(pointsGroups, aggregator, timeSeriesQuery.isRate))
            .map(seriesMap => pointsToString(timeSeriesQuery.metricName, seriesMap))
        }

      for (responseFuture <- resultFutureOrErrMessage.right) {
        responseFuture
          .map(result => savedClient ! HttpResponse(entity = HttpEntity(result)))
          .onFailure {
          case t: Throwable =>
            savedClient ! HttpResponse(status = StatusCodes.InternalServerError)
            info(f"query failed", t)
        }
      }
      for (errMessage <- resultFutureOrErrMessage.left) {
        savedClient ! HttpResponse(status = StatusCodes.BadRequest, entity = HttpEntity(errMessage))
        info(errMessage)
      }


    case request: HttpRequest =>
      info(f"request: $request)")
      sender ! HttpResponse(entity = HttpEntity("not implemented"))
  }
}

object OpenTSDBHttpApiServer extends HttpServerFactory {

  val aggregatorsMap = Map[String, Seq[Double] => Double](
    "sum" -> (seq => seq.sum),
    "min" -> (seq => seq.min),
    "max" -> (seq => seq.max),
    "avg" -> (seq => seq.sum / seq.size)
  )

  val dateFormat = new SimpleDateFormat("yyyy/MM/dd-hh:mm:ss")

  private val metricNameRegex = "[^{]+".r
  private val metricTagsRegex = """[^{]+\{([^}]+)\}""".r
  private val metricSingleTagRegex = "[^,]+".r

  case class TimeSeriesQuery(aggregatorKey: String, isRate: Boolean, metricName: String, tags: Map[String, ValueNameFilterCondition])

  def runServer(config: Config, storage: PointsStorage, system: ActorSystem, executionContext: ExecutionContext) {
    implicit val timeout: Timeout = 5 seconds
    val handler = system.actorOf(
      Props.apply(new OpenTSDBHttpApiServer(storage, executionContext)),
      name = "server-http")

    val hostport = config.getString("server.http.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    val relaxedUriParsingSettings = """
                                      |spray.can.server.parsing {
                                      |  uri-parsing-mode = relaxed-with-raw-query
                                      |}
                                    """.stripMargin
    IO(Http)(system) ? Http.Bind(
      listener = handler,
      endpoint = addr,
      backlog = config.getInt("server.http.backlog"),
      options = Nil,
      settings = Some(ServerSettings(relaxedUriParsingSettings)))
  }

  private val paramRegex = "[^&]+".r

  private[query] def queryStringToMap(queryString: String): Map[String, String] =
    paramRegex.findAllIn(queryString).map {
      parameter =>
        val equalsIndex = parameter.indexOf('=')
        if (equalsIndex != -1) {
          (parameter.substring(0, equalsIndex), parameter.substring(equalsIndex + 1))
        } else {
          (parameter, "")
        }
    }.toMap

  private[query] def parseTime(dateString: String) = (dateFormat.parse(dateString).getTime / 1000).toInt

  private[query] def parseTimeSeriesQuery(queryString: String): Either[String, TimeSeriesQuery] = {

    val queryPartsArray = queryString.split(":")
    case class QueryParts(aggregatorKey: String, isRate: Boolean, metric: String)
    val errorMessageOrQueryParts =
      if (queryPartsArray.length >= 2) {
        val arrayLength = queryPartsArray.length
        Right(QueryParts(
          aggregatorKey = queryPartsArray.head,
          isRate = queryPartsArray.slice(1, arrayLength - 1).exists(_ == "rate"),
          metric = queryPartsArray.last
        ))
      } else {
        Left("bad metric string: less than 2 tokens")
      }

    for {
      queryParts <- errorMessageOrQueryParts.right
      metricName <- metricNameFromMetricString(queryParts.metric).right
      tags <- tagsFromMetricString(queryParts.metric).right
    } yield {
      TimeSeriesQuery(queryParts.aggregatorKey, queryParts.isRate, metricName, tags)
    }
  }

  private def metricNameFromMetricString(metricString: String) = metricNameRegex.findPrefixOf(metricString).toRight("empty metric")

  private def tagsFromMetricString(metricString: String): Either[String, Map[String, ValueNameFilterCondition]] = {
    val tagsOption = metricTagsRegex.findPrefixMatchOf(metricString).map(_.group(1))
    tagsOption.map {
      tagsString =>
        val tags = metricSingleTagRegex.findAllIn(tagsString).toList
        val splittedTags = tags.map(_.split("="))
        if (splittedTags.forall(_.length == 2)) {
          val nameValuePairs = splittedTags.map {
            array => (array(0), parseValueFilterCondition(array(1)))
          }
          Right(nameValuePairs.toMap)
        } else {
          Left("wrong tags format")
        }
    }.getOrElse(Right(Map()))
  }

  private def parseValueFilterCondition(valueString: String) = {
    if (valueString == "*") {
      All
    } else if (valueString.contains("|")) {
      Multiple(valueString.split("\\|").toList)
    } else {
      Single(valueString)
    }
  }

  private def aggregatePoints(pointsGroups: PointsGroups,
                              interpolationAggregator: Seq[Double] => Double,
                              rate: Boolean): Map[PointAttributes, Seq[(Int, Double)]] = {

    def toGraphPointIterator(points: Seq[Point]) = points.iterator.map {
      point => (point.timestamp, point.value.doubleValue())
    }
    pointsGroups.groupsMap.mapValues {
      group =>
        val graphPointIterators = group.values.map(toGraphPointIterator).toSeq
        val aggregated = PointSeriesUtils.interpolateAndAggregate(graphPointIterators, interpolationAggregator)
        val result =
          if (rate) {
            PointSeriesUtils.differentiate(aggregated)
          } else {
            aggregated
          }
        result.toList
    }
  }

  private def pointsToString(metricName: String, allSeries: Map[PointAttributes, Seq[(Int, Double)]]) = {
    val lines = for {
      (attributes, points) <- allSeries.iterator
      attributesString = attributes.map { case (name, value) => f"$name=$value"}.mkString(" ")
      (timestamp, value) <- points
    } yield f"$metricName $timestamp $value $attributesString"
    lines.mkString("\n")
  }
}