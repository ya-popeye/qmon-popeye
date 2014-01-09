package popeye.intergrationtests

import popeye.storage.hbase.PointsStorageStub
import akka.actor.ActorSystem
import popeye.proto.{PackedPoints, Message}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import popeye.query.{PointsStorage, OpenTSDBHttpApiServer}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable
import popeye.query.PointsStorage.NameType
import popeye.storage.hbase.HBaseStorage.{PointsStream, ValueNameFilterCondition}
import popeye.Logging

class OpenTSDBHttpApiServerTest extends Logging {
  implicit val actorSystem = ActorSystem("test")
  implicit val eCtx = actorSystem.dispatcher
  val storageStub = new PointsStorageStub

  val metrics = mutable.Set[String]()
  val attrNames = mutable.Set[String]()
  val attrValues = mutable.Set[String]()

  def test(args: Array[String]) {
    writePoints("sin", i => math.sin(i * 0.01) + 1)("phase" -> "0", "freq" -> "0.01", "add" -> "1")
    startServer()
  }

  def startServer() {
    val serverConfig = ConfigFactory.parseString(
      """
        |server.http {
        |  listen = "localhost:8080"
        |  backlog = 100
        |}
      """.stripMargin)
    val pointsStorage = new PointsStorage {
      def getPoints(metric: String,
                    timeRange: (Int, Int),
                    attributes: Map[String, ValueNameFilterCondition]): Future[PointsStream] =
        storageStub.storage.getPoints(metric, timeRange, attributes)(eCtx)

      def getSuggestions(namePrefix: String, nameType: NameType.NameType): Seq[String] = {
        import NameType._
        val names = nameType match {
          case MetricType => metrics
          case AttributeNameType => attrNames
          case AttributeValueType => attrValues
        }
        val suggestions = names.toList.filter(name => name.startsWith(namePrefix)).sorted.take(10)
        info(f"suggestions $suggestions")
        suggestions
      }
    }
    OpenTSDBHttpApiServer.runServer(serverConfig, pointsStorage, actorSystem, eCtx)
  }

  def stopServer() {
    actorSystem.shutdown()
  }

  def writePoints(metricName: String, value: Int => Double)(tags: (String, String)*) {
    val numberOfPoints = 3600
    val timeIntervalInHours = 10
    val currentTime = System.currentTimeMillis() / 1000
    val points =
      for (i <- 1 to numberOfPoints) yield {
        val timestamp = currentTime - (numberOfPoints - i) * (timeIntervalInHours * 3600 / numberOfPoints)
        messagePoint(metricName, timestamp, value(i), tags)
      }
    addMetric(metricName)
    addTags(tags)
    val writeFuture = storageStub.storage.writePoints(PackedPoints(points))
    Await.ready(writeFuture, 5 seconds)
  }

  def messagePoint(metricName: String, timestamp: Long, value: Double, attrs: Seq[(String, String)]) = {
    val builder = Message.Point.newBuilder()
      .setMetric(metricName)
      .setTimestamp(timestamp)
      .setFloatValue(value.toFloat)
    for ((name, value) <- attrs) {
      builder.addAttributes(attribute(name, value))
    }
    builder.build()
  }

  def addMetric(metricName: String) {
    if (!metrics.contains(metricName)) {
      metrics += metricName
      storageStub.addMetric(metricName)
    }
  }

  def addTags(tags: Seq[(String, String)]) = {
    for ((name, value) <- tags) {
      if (!attrNames.contains(name)) {
        attrNames += name
        storageStub.addAttrName(name)
      }
      if (!attrValues.contains(value)) {
        attrValues += value
        storageStub.addAttrValue(value)
      }
    }
  }

  def attribute(name: String, value: String) =
    Message.Attribute.newBuilder().setName(name).setValue(value).build()
}
