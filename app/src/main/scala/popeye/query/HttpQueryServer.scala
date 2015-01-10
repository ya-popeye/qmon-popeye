package popeye.query

import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import popeye.Logging
import spray.http._
import spray.http.HttpMethods._
import spray.can.Http
import scala.concurrent.{Promise, ExecutionContext}
import popeye.storage.hbase.HBaseStorage._
import popeye.storage.ValueNameFilterCondition
import scala.util.Try
import java.io.{PrintWriter, StringWriter}
import HttpQueryServer._
import spray.http.HttpResponse
import popeye.storage.hbase.HBaseStorage.PointsGroups
import spray.http.HttpRequest


class HttpQueryServer(storage: PointsStorage, executionContext: ExecutionContext) extends Actor with Logging {
  implicit val eCtx = executionContext
  val pointsPathPattern = """/points/([^/]+)""".r

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
        val unfulfillablePromise = Promise()
        val pointsGroupsFuture = storage.getPoints(
          metricName,
          (startTime.toInt, endTime.toInt),
          attributes,
          unfulfillablePromise.future
        )
        val aggregatedPointsFuture = pointsGroupsFuture.map {
          groups => aggregationsToString(aggregatePoints(groups, aggregation, downsampling))
        }
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
              AllValueNames
            } else if (valuesString.contains(" ")) {
              MultipleValueNames(valuesString.split(" "))
            } else {
              SingleValueName(valuesString)
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

  override def createHandler(system: ActorSystem,
                             storage: PointsStorage,
                             executionContext: ExecutionContext): ActorRef = {
    system.actorOf(Props.apply(new HttpQueryServer(storage, executionContext)))
  }

  private def aggregationsToString(aggregationsMap: Map[PointAttributes, Seq[(Int, Double)]]): String =
    aggregationsMap.toList
      .flatMap { case (attrs, points) => attrs +: points}
      .mkString("\n")


  private def aggregatePoints(pointsGroups: PointsGroups,
                              interpolationAggregator: Seq[Double] => Double,
                              downsamplingOption: Option[(Int, Seq[Double] => Double)]): Map[PointAttributes, Seq[(Int, Double)]] = {
    OpenTSDB2HttpApiServer.aggregatePoints(pointsGroups, interpolationAggregator, false, downsamplingOption)
  }
}
