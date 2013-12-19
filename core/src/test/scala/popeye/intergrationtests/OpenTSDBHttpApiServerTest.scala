package popeye.intergrationtests

import popeye.storage.hbase.PointsStorageStub
import akka.actor.ActorSystem
import popeye.proto.{PackedPoints, Message}
import scala.concurrent.Await
import scala.concurrent.duration._
import popeye.query.{PointsStorage, OpenTSDBHttpApiServer}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable

object OpenTSDBHttpApiServerTest {
  implicit val actorSystem = ActorSystem("test")
  implicit val eCtx = actorSystem.dispatcher
  val storageStub = new PointsStorageStub

  val metrics = mutable.Set[String]()
  val attrNames = mutable.Set[String]()
  val attrValues = mutable.Set[String]()

  def main(args: Array[String]) {
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
    val pointsStorage = PointsStorage.fromHBaseStorage(storageStub.storage, eCtx)
    OpenTSDBHttpApiServer.runServer(serverConfig, pointsStorage, actorSystem, eCtx)
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
