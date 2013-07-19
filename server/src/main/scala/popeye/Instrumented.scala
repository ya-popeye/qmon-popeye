package popeye

import nl.grons.metrics.scala._
import com.codahale.metrics.{MetricRegistry,Gauge => CHGauge}
import nl.grons.metrics.scala.Gauge
import akka.actor.{ActorRef, Actor}

/**
 * Builds and registering metrics.
 */
class ActorMetricBuilder(val owner: ActorRef, val registry: MetricRegistry) {

  private[this] def metricName(name: String, scope: String = null): String =
    if (scope == null) MetricRegistry.name(owner.path.toString, name)
    else MetricRegistry.name(owner.path.toString, name, scope)

  /**
   * Registers a new gauge metric.
   *
   * @param name  the name of the gauge
   * @param scope the scope of the gauge or null for no scope
   */
  def gauge[A](name: String, scope: String = null)(f: => A): Gauge[A] =
    Gauge[A](registry.register(metricName(name, scope), new CHGauge[A] { def getValue: A = f }))

  /**
   * Creates a new counter metric.
   *
   * @param name  the name of the counter
   * @param scope the scope of the counter or null for no scope
   */
  def counter(name: String, scope: String = null): Counter =
    new Counter(registry.counter(metricName(name, scope)))

  /**
   * Creates a new histogram metrics.
   *
   * @param name   the name of the histogram
   * @param scope  the scope of the histogram or null for no scope
   */
  def histogram(name: String, scope: String = null): Histogram =
    new Histogram(registry.histogram(metricName(name, scope)))

  /**
   * Creates a new meter metric.
   *
   * @param name the name of the meter
   * @param scope the scope of the meter or null for no scope
   */
  def meter(name: String, scope: String = null): Meter =
    new Meter(registry.meter(metricName(name, scope)))

  /**
   * Creates a new timer metric.
   *
   * @param name the name of the timer
   * @param scope the scope of the timer or null for no scope
   */
  def timer(name: String, scope: String = null): Timer =
    new Timer(registry.timer(metricName(name, scope)))

}


/**
 * @author Andrey Stepachev
 */
trait Instrumented { self: Actor =>

  private lazy val metricBuilder = new ActorMetricBuilder(this.self, metricRegistry)

  /**
   * The MetricBuilder that can be used for creating timers, counters, etc.
   */
  def metrics: ActorMetricBuilder = metricBuilder

  /**
   * The MetricRegistry where created metrics are registered.
   */
  val metricRegistry: MetricRegistry

}
