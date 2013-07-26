package popeye.transport

import popeye.transport.kafka.KafkaEventProducer
import popeye.transport.legacy.{LegacyHttpHandler, TsdbTelnetServer}
import popeye.uuid.IdGenerator

/**
 * @author Andrey Stepachev
 */
object SlicerMain extends PopeyeMain("slicer") {

  implicit val idGenerator = new IdGenerator(
    config.getLong("generator.worker"),
    config.getLong("generator.datacenter")
  )

  val kafkaProducer = KafkaEventProducer.start(config, idGenerator)

  TsdbTelnetServer.start(config, kafkaProducer)
  LegacyHttpHandler.start(config, kafkaProducer)

}
