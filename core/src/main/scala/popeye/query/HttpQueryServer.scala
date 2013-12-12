package popeye.query

import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import popeye.Logging
import spray.http._
import spray.http.HttpMethods._
import akka.pattern.ask
import java.net.InetSocketAddress
import akka.io.IO
import spray.can.Http
import com.typesafe.config.Config
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import popeye.storage.hbase.HBaseStorage
import popeye.storage.hbase.HBaseStorage._
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition
import scala.util.Try
import java.io.{PrintWriter, StringWriter}
import HttpQueryServer._
import popeye.storage.hbase.HBaseStorage.Point
import spray.http.HttpResponse
import popeye.storage.hbase.HBaseStorage.PointsGroups
import spray.http.HttpRequest


class HttpQueryServer(storage: PointsStorage, executionContext: ExecutionContext) extends Actor with Logging {
  implicit val eCtx = executionContext
  val pointsPathPattern = """/points/([^/]+)""".r

  case class SendNextPoints(client: ActorRef, points: PointsStream)

  def receive: Actor.Receive = {
    case x: Http.Connected => sender ! Http.Register(self)

    case request@HttpRequest(GET, Uri.Path(pointsPathPattern(metricName)), _, _, _) =>
      val savedClient = sender
      val query = request.uri.query
      val pointsQuery = for {
        startTime <- query.get("start").toRight("start is not set").right
        endTime <- query.get("end").toRight("end is not set").right
        attributesString <- query.get("attrs").toRight("attributes are not set").right
        attributes <- parseAttributes(attributesString).right
        aggregation <- parseInterpolationAggregator(query).right
        downsampling <- Right(parseDownsampling(query)).right
      } yield {
        val pointStreamFuture = storage.getPoints(metricName, (startTime.toInt, endTime.toInt), attributes)
        val aggregatedPointsFuture =
          pointStreamFuture
            .flatMap(_.toFuturePointsGroups)
            .map(groups => aggregationsToString(aggregatePoints(groups, aggregation, downsampling)))
        aggregatedPointsFuture.onComplete {
          tryString =>
            tryString.map {
              string => savedClient ! HttpResponse(entity = HttpEntity(string))
            }.recoverWith {
              case t: Throwable => Try {
                val sw = new StringWriter()
                t.printStackTrace(new PrintWriter(sw))
                val errMessage = sw.toString
                savedClient ! HttpResponse(entity = HttpEntity(errMessage))
              }
            }
        }
      }

      for (errorMessage <- pointsQuery.left) {
        savedClient ! HttpResponse(entity = HttpEntity(errorMessage))
      }

    case HttpRequest(GET, Uri.Path(path), _, _, _) =>
      sender ! f"invalid path: $path"

  }

  def parseDownsampling(query: Uri.Query): Option[(Int, Seq[Double] => Double)] = for {
    dsAggregationKey <- query.get("dsagrg")
    dsAggregation <- aggregatorsMap.get(dsAggregationKey)
    dsInterval <- query.get("dsint")
  } yield (dsInterval.toInt, dsAggregation)

  def parseInterpolationAggregator(query: Uri.Query) = for {
    aggregationKey <- query.get("agrg").toRight("aggregation is not set").right
    aggregation <- aggregatorsMap.get(aggregationKey)
      .toRight(f"invalid aggregation, available aggregations:${aggregatorsMap.keys}").right
  } yield aggregation

  def parseAttributes(attributesString: String): Either[String, Map[String, ValueNameFilterCondition]] =
    Try {
      import ValueNameFilterCondition._
      val attributes = attributesString.split(";").toList.filter(!_.isEmpty)
      val valueFilters = attributes.map {
        attrPair =>
          val Array(name, valuesString) = attrPair.split("->")
          val valueFilterCondition =
            if (valuesString == "*") {
              All
            } else if (valuesString.contains(" ")) {
              Multiple(valuesString.split(" "))
            } else {
              Single(valuesString)
            }
          (name, valueFilterCondition)
      }
      valueFilters.toMap
    }.toOption.toRight("incorrect attributes format; example: \"?attrs=host->foo;type->bar+foo;port->*\"")

}

object HttpQueryServer extends HttpServerFactory {

  val aggregatorsMap = Map[String, Seq[Double] => Double](
    "sum" -> (seq => seq.sum),
    "min" -> (seq => seq.min),
    "max" -> (seq => seq.max),
    "avg" -> (seq => seq.sum / seq.size)
  )

  def runServer(config: Config, storage: HBaseStorage, system: ActorSystem, executionContext: ExecutionContext) = {
    implicit val timeout: Timeout = 5 seconds
    val pointsStorage = PointsStorage.fromHBaseStorage(storage, executionContext)
    val handler = system.actorOf(
      Props.apply(new HttpQueryServer(pointsStorage, executionContext)),
      name = "server-http")

    val hostport = config.getString("server.http.listen").split(":")
    val addr = new InetSocketAddress(hostport(0), hostport(1).toInt)
    IO(Http)(system) ? Http.Bind(
      listener = handler,
      endpoint = addr,
      backlog = config.getInt("server.http.backlog"),
      options = Nil,
      settings = None)
    handler
  }

  private def aggregationsToString(aggregationsMap: Map[PointAttributes, Seq[(Int, Double)]]): String =
    aggregationsMap.toList
      .flatMap { case (attrs, points) => attrs +: points}
      .mkString("\n")


  private def aggregatePoints(pointsGroups: PointsGroups,
                              interpolationAggregator: Seq[Double] => Double,
                              downsamplingOption: Option[(Int, Seq[Double] => Double)]): Map[PointAttributes, Seq[(Int, Double)]] = {
    def toGraphPointIterator(points: Seq[Point]) = {
      val graphPoints = points.iterator.map {
        point => (point.timestamp, point.value.doubleValue())
      }
      downsamplingOption.map {
        case (interval, aggregator) => PointAggregation.downsample(graphPoints, interval, aggregator)
      }.getOrElse(graphPoints)
    }
    pointsGroups.groupsMap.mapValues {
      group =>
        val graphPointIterators = group.values.map(toGraphPointIterator).toSeq
        PointAggregation.linearInterpolation(graphPointIterators, interpolationAggregator).toList
    }
  }
}
