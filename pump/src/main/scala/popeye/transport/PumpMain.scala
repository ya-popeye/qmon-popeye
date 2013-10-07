package popeye.transport

import popeye.storage.hbase._


/**
 * @author Andrey Stepachev
 */
object PumpMain extends PopeyeMain("pump") {

  val pointsStorage = new PointsStorageConfigured(new PointsStorageConfig(
    actorSystem = PumpMain.actorSystem,
    metricRegistry = PumpMain.metricRegistry,
    config = PumpMain.config.getConfig("hbase"),
    zkQuorum = PumpMain.config.getString("zk.cluster")
  ))

  val consumer = HBasePointConsumer.start(config, pointsStorage.storage)
  log.info("Pump initialization done")
}
