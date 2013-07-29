package popeye.transport

import popeye.storage.opentsdb.TsdbPointConsumer

/**
 * @author Andrey Stepachev
 */
object PumpMain extends PopeyeMain("pump") {
  val consumer = TsdbPointConsumer.start(config)

}
