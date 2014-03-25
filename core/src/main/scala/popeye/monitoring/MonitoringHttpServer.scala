package popeye.monitoring

import akka.util.Timeout
import akka.actor.{Actor, Props, ActorSystem}
import akka.pattern.ask
import scala.concurrent.duration._
import akka.io.IO
import spray.can.Http
import java.net.InetSocketAddress
import spray.http.HttpEntity
import popeye.Logging
import com.codahale.metrics._
import spray.http.HttpRequest
import spray.http.HttpResponse

class MonitoringHttpServerActor(metricRegistry: MetricRegistry) extends Actor with Logging {
  override def receive: Actor.Receive = {
    case x: Http.Connected =>
      sender ! Http.Register(self)
    case request: HttpRequest =>
      val metricsText: String = (getSlicerMetrics() ++ getPumpMetrics()).map {
        case (name, value) => f"$name=$value"
      }.mkString("\n")
      sender ! HttpResponse(entity = HttpEntity(metricsText))
  }

  def hasMetric(nameSuffix: String) = {
    !metricRegistry.getMeters(metricFilter(_.endsWith(nameSuffix))).isEmpty
  }

  def findTimers(nameSuffix: String): Seq[(String, Timer)] = {
    import scala.collection.JavaConverters._
    metricRegistry.getTimers(metricFilter(_.endsWith(nameSuffix))).asScala.toList
  }

  def getMeter(name: String) = {
    val meters = metricRegistry.getMeters(metricFilter(_.endsWith(name)))
    require(meters.size == 1, f"meter $name is not initialized or has duplicates : ${meters.keySet()}")
    meters.values().iterator().next()
  }

  def getTimer(name: String) = {
    val meters = metricRegistry.getTimers(metricFilter(_.endsWith(name)))
    require(meters.size == 1, f"meter $name is not initialized or has duplicates : ${meters.keySet()}")
    meters.values().iterator().next()
  }

  def getPumpMetrics(): Seq[(String, String)] = {
    def getTimeValues(suffix: String): Seq[(String, String)] = {
      val timers = findTimers(suffix)
      timers.flatMap {
        case (name, timer) =>
          val receiveTimeNanos50p = (timer.getSnapshot.getMedian.toLong / 1000 / 1000).toString
          val receiveTimeNanos95p = (timer.getSnapshot.get95thPercentile().toLong / 1000 / 1000).toString
          Seq(f"$name.50p" -> receiveTimeNanos50p, f"$name.95p" -> receiveTimeNanos95p)
      }
    }
    val consumeTimeValues = getTimeValues("consume.time")
    val hbaseTimeTimers = getTimeValues("write.hbase.time")
    (consumeTimeValues.toList ++ hbaseTimeTimers.toList).sortBy(_._1)
  }

  def getSlicerMetrics(): Seq[(String, String)] = {
    if (!hasMetric("kafka.producer.batch-complete")) return Nil

    val completeBatches = getMeter("kafka.producer.batch-complete").getOneMinuteRate
    val failedBatches = getMeter("kafka.producer.batch-failed").getOneMinuteRate
    val sendTimeSnapshot = getTimer("kafka.producer.send-time").getSnapshot
    val sendTimeNanos50p = sendTimeSnapshot.getMedian.toLong
    val sendTimeNanos95p = sendTimeSnapshot.get95thPercentile.toLong
    Seq(
      "slicer.kafka.failed_batches_percentage" -> ((failedBatches / (completeBatches + 1)) * 100).toInt.toString,
      "slicer.kafka.send-time.50p" -> (sendTimeNanos50p / 1000 / 1000).toString,
      "slicer.kafka.send-time.95p" -> (sendTimeNanos95p / 1000 / 1000).toString
    )
  }

  def metricFilter(f: String => Boolean) = new MetricFilter {
    override def matches(name: String, metric: Metric): Boolean = f(name)
  }
}

object MonitoringHttpServer extends Logging{
  def runServer(address: InetSocketAddress, metricRegistry: MetricRegistry, system: ActorSystem) = {
    implicit val timeout: Timeout = 5 seconds
    val handler = system.actorOf(
      Props.apply(new MonitoringHttpServerActor(metricRegistry)),
      name = "monitoring-server-http")
    info("MonitoringHttpServer starting")
    IO(Http)(system) ? Http.Bind(
      listener = handler,
      endpoint = address,
      backlog = 100,
      options = Nil,
      settings = None
    )
  }
}
