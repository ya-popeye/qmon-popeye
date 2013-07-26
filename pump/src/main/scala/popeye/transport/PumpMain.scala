package popeye.transport

import popeye.storage.opentsdb.TsdbEventConsumer

/**
 * @author Andrey Stepachev
 */
object PumpMain extends PopeyeMain("pump") {
  val consumer = TsdbEventConsumer.start(config)

}
