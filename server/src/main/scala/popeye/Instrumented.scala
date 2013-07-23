package popeye

import nl.grons.metrics.scala._
import com.codahale.metrics.{MetricRegistry,Gauge => CHGauge}
import nl.grons.metrics.scala.Gauge
import akka.actor.{ActorRef, Actor}

/**
 * @author Andrey Stepachev
 */
trait Instrumented extends InstrumentedBuilder {

}
