package popeye.transport

import popeye.transport.kafka.KafkaPointsProducer
import popeye.IdGenerator
import popeye.transport.server.http.HttpPointsServer
import popeye.transport.server.telnet.TsdbTelnetServer

/**
 * @author Andrey Stepachev
 */
object SlicerMain extends PopeyeMain("slicer") {

  implicit val idGenerator = new IdGenerator(
    config.getLong("generator.worker"),
    config.getLong("generator.datacenter")
  )

  val kafkaProducer = KafkaPointsProducer.start("kafka", config, idGenerator)

  TsdbTelnetServer.start(config, kafkaProducer)
  HttpPointsServer.start(config, kafkaProducer)

}
