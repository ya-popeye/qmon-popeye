package popeye.transport.bench

import scala.concurrent.duration._
import akka.util.Timeout
import akka.actor._
import akka.pattern.{pipe, ask}
import com.typesafe.config.ConfigFactory
import com.codahale.metrics.{Meter, ConsoleReporter, Timer, MetricRegistry}
import akka.event.LogSource
import spray.http._
import java.util.concurrent.TimeUnit
import spray.can.Http
import spray.http.HttpMethods.POST
import akka.io.IO
import spray.http.HttpRequest
import scala.Some
import spray.http.HttpResponse
import spray.can.Http.HostConnectorInfo

/**
 * @author Andrey Stepachev
 */
object Main extends App {
  implicit val timeout: Timeout = 2 seconds
  implicit val system = ActorSystem("popeye",
    ConfigFactory.parseResources("application.conf")
      .withFallback(ConfigFactory.parseResources("dynamic.conf"))
      .withFallback(ConfigFactory.load())
      .resolve()
  )

  implicit val metricRegistry = new MetricRegistry()

  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName

    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }
  val log = akka.event.Logging(system, this)


  val responseTimer = metricRegistry.timer("http-time")
  val requestsMeter = metricRegistry.meter("http-requests.total")
  val failedRequestsMeter = metricRegistry.meter("http-requests.failed")

  import scala.concurrent.ExecutionContext.Implicits.global
  for (
    HostConnectorInfo(hostConnector, _) <- IO(Http).ask(Http.HostConnectorSetup("localhost", port = 8080))
  ) yield {
    for (i <- 1 to 2) {
      system.actorOf(Props(new Worker(
        metricRegistry,
        responseTimer,
        requestsMeter,
        failedRequestsMeter,
        hostConnector)), name = "http-client" + i)
    }
  }

  val reporter = ConsoleReporter.forRegistry(metricRegistry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build();
  reporter.start(5, TimeUnit.SECONDS);

}

class Worker(metricRegistry: MetricRegistry,
             timer: Timer, totalMeter: Meter, failedMeter: Meter,
             hostConnector: ActorRef) extends Actor with ActorLogging {

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val timeout: Timeout = 10 seconds
  var timeContext: Option[Timer.Context] = None
  val meter = metricRegistry.meter(self.path.toString + "/requests")

  override def preStart() {
    super.preStart()
    makeReq()
  }

  def receive = {
    case x@HttpResponse(code, _, _, _) =>
      if (code == StatusCodes.OK) {
        timeContext foreach {
          _.stop
        }
      } else {
        failedMeter.mark()
      }
      totalMeter.mark()
      makeReq()
  }

  private val metricName = "HOSTNAME/" + self.path.toString.replaceAll("[^a-zA-Z0-9]", "_")

  private def mkMetric(value: Long, timestamp: Long) = {
    "{\"" + metricName +
      "\": [{\"type\": \"numeric\", \"timestamp\": " + timestamp + ", " + "\"value\": " + value + "}]}"
  }

  private def makeReq() = {
    timeContext = Some(timer.time())
    meter.mark()

    hostConnector.ask(HttpRequest(POST, "/write", Nil,
      HttpEntity(ContentTypes.`application/json`,
        mkMetric(meter.getCount, System.currentTimeMillis()))))(timeout)
      .mapTo[HttpResponse] pipeTo self
  }
}
