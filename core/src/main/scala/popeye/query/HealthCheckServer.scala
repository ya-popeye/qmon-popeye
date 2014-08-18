package popeye.query

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.actor.{Props, ActorSystem, Actor}
import popeye.Logging
import spray.http._
import scala.util.Try
import popeye.storage.hbase.HBaseStorage.PointsStream
import scala.concurrent.duration._
import spray.http.HttpRequest
import spray.http.HttpResponse
import com.typesafe.config.Config
import akka.util.Timeout
import akka.io.IO
import akka.pattern.ask
import spray.can.Http
import java.net.InetSocketAddress

class HealthCheckServer(storage: PointsStorage, executionContext: ExecutionContext) extends Actor with Logging {

  private case class ParsedQuery(metric: String,
                                 timeInterval: Int,
                                 fixedAttrs: Seq[(String, String)],
                                 countAttr: String)

  implicit val ectx = executionContext

  override def receive: Actor.Receive = {
    case x: Http.Connected => sender ! Http.Register(self)
    case request: HttpRequest =>
      val savedClient = sender
      val query = request.uri.query
      val errorMessageOrParsedQuery = parseQuery(query)
      val responseFuture = errorMessageOrParsedQuery.fold(
        getParsingErrorMessageResponse,
        getHealthStatusResponse
      )
      responseFuture.map {
        response => savedClient ! response
      }
  }

  def getParsingErrorMessageResponse(errMessage: String): Future[HttpResponse] = {
    val responseString = "{ error : \"" + errMessage + "\" }"
    val response = HttpResponse(status = StatusCodes.BadRequest, entity = HttpEntity(responseString))
    Future.successful(response)
  }

  def getHealthStatusResponse(query: ParsedQuery): Future[HttpResponse] = {
    val currentTime = (System.currentTimeMillis() / 1000).toInt
    val future = HealthCheckTool.checkHealth(
      storage,
      query.metric,
      query.fixedAttrs,
      query.countAttr,
      checkTime = currentTime,
      timeInterval = query.timeInterval
    )
    future.map {
      isHealty =>
        val responseString = "{ isHealthy : \"" + isHealty + "\" }"
        HttpResponse(entity = HttpEntity(responseString))
    }.recover {
      case e: Exception =>
        log.error("health check failed", e)
        val responseString = "{ error : \"" + e.getMessage + "\" }"
        HttpResponse(status = StatusCodes.InternalServerError, entity = HttpEntity(responseString))
    }
  }

  private def parseQuery(query: Uri.Query): Either[String, ParsedQuery] = {
    for {
      metric <- query.get("metric").toRight("metric is not set").right
      timeInterval <- query.get("time_interval").toRight("time interval is not set").right.flatMap(parseInt).right
      fixedAttrs <- parseFixedAttributes(query).right
      countAttrName <- query.get("count_attr").toRight("count attribute is not set").right
    } yield {
      ParsedQuery(metric, timeInterval, fixedAttrs, countAttrName)
    }
  }

  def parseInt(string: String): Either[String, Int] = {
    Try {string.toInt}.toOption.toRight(f"bad number: $string")
  }

  def parseAttribute(attrString: String) = {
    val splitted = attrString.split(" ")
    val isValidFormat = attrString.length == 2
    if (isValidFormat) {
      Right(splitted(0), splitted(1))
    } else {
      Left(f"wrong attribute format: '$attrString', example: 'name value,foo bar'")
    }
  }

  def parseFixedAttributes(query: Uri.Query): Either[String, Seq[(String, String)]] = {
    query.get("fixed_attrs").map {
      attrsString =>
        val attrsStrings = if (attrsString.isEmpty) {
          Seq.empty
        } else {
          attrsString.split(",").map(_.split(" ").toSeq).toSeq
        }
        val isValidFormat = attrsStrings.forall(_.length == 2)
        if (isValidFormat) {
          val nameValuePairs = attrsStrings.map {
            array => (array(0), array(1))
          }.toList
          Right(nameValuePairs)
        } else {
          Left(f"wrong attribute format: '$attrsString', example: 'name value,foo bar'")
        }
    }.getOrElse(Right(Seq.empty))
  }
}

object HealthCheckServer extends HttpServerFactory {
  def runServer(config: Config, storage: PointsStorage, system: ActorSystem, executionContext: ExecutionContext) {
    implicit val timeout: Timeout = 5 seconds
    val handler = system.actorOf(
      Props.apply(new HealthCheckServer(storage, executionContext)),
      name = "health-check-server-http")

    val hostAndPort = config.getString("http.listen").split(":")
    IO(Http)(system) ? Http.Bind(
      listener = handler,
      endpoint = new InetSocketAddress(hostAndPort(0), hostAndPort(1).toInt),
      backlog = config.getInt("http.backlog"),
      options = Nil,
      settings = None
    )
  }
}

object HealthCheckTool {
  val DistinctTagsDropThreshold: Double = 0.3

  def checkHealth(pointsStorage: PointsStorage,
                  metric: String,
                  fixedAttributes: Seq[(String, String)],
                  countAttribute: String,
                  checkTime: Int,
                  timeInterval: Int)
                 (implicit ectx: ExecutionContext): Future[Boolean] = {
    val nameValueConditions = createNameValueConditions(fixedAttributes, countAttribute)
    def countDistinctTagValues(startStopTime: (Int, Int)): Future[Int] = {
      val pointsStreamFuture = pointsStorage.getPoints(metric, startStopTime, nameValueConditions)
      for {
        pointsStream <- pointsStreamFuture
        distinctValues <- getAllDistinctAttributeValues(pointsStream)
      } yield {
        distinctValues.size
      }
    }
    val firstInterval = (checkTime - timeInterval * 3, checkTime - timeInterval * 2)
    val secondInterval = (checkTime - timeInterval * 2, checkTime - timeInterval)
    for {
      firstCount <- countDistinctTagValues(firstInterval)
      secondCount <- countDistinctTagValues(secondInterval)
    } yield {
      (firstCount - secondCount).toDouble / firstCount < DistinctTagsDropThreshold
    }
  }

  def getAllDistinctAttributeValues(pointsStream: PointsStream)(implicit ectx: ExecutionContext): Future[Set[String]] = {
    val attrSetsStream = pointsStream.mapElements {
      pointsGroup =>
      val attrs = pointsGroup.groupsMap.keys
      val attrNames = attrs.flatMap(_.keys)
      require(attrNames.size == 1, f"should be exactly one \'group by\' attribute name, not ${attrNames.size}")
        attrs.flatMap(_.values).toSet
    }
    attrSetsStream.reduceElements(_ ++ _)
  }

  private def createNameValueConditions(fixedAttrs: Seq[(String, String)], countAttrName: String) = {
    import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition._
    val fixedAttrsConditions = fixedAttrs.map {case (name, value) => (name, SingleValueName(value))}
    ((countAttrName, AllValueNames) +: fixedAttrsConditions).toMap
  }

}
