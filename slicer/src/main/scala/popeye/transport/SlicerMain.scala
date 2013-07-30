package popeye.transport

import popeye.transport.kafka.KafkaPointProducer
import popeye.transport.legacy.{LegacyHttpHandler, TsdbTelnetServer}
import popeye.IdGenerator

/**
 * @author Andrey Stepachev
 */
object SlicerMain extends PopeyeMain("slicer") {

  implicit val idGenerator = new IdGenerator(
    config.getLong("generator.worker"),
    config.getLong("generator.datacenter")
  )

  val kafkaProducer = KafkaPointProducer.start(config, idGenerator)

  TsdbTelnetServer.start(config, kafkaProducer)
  LegacyHttpHandler.start(config, kafkaProducer)

}
