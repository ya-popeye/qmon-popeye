package popeye.transport.test

import kafka.server.{KafkaRequestHandler, KafkaServer, KafkaConfig}
import kafka.common.KafkaException
import kafka.utils.{Utils, TestUtils}
import org.apache.log4j.Logger


/**
 * @author Andrey Stepachev
 */
trait KafkaServerTestSpec extends ZkTestSpec {
  def numServers(): Int = 1

  val kafkaBrokerConfigs = TestUtils.createBrokerConfigs(numServers()).map({
    conf => new KafkaConfig(conf)
  })
  val kafkaRequestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
  var kafkaBrokers: List[KafkaServer] = null

  def kafkaBrokersList = kafkaBrokers.map({
    s => s.socketServer.host + ":" + s.socketServer.port
  }).mkString(",")

  def withKafkaServer()(body: => Unit) {
    withZk() {
      if (kafkaBrokerConfigs.size <= 0)
        throw new KafkaException("Must suply at least one server config.")
      kafkaBrokers = kafkaBrokerConfigs.map(TestUtils.createServer(_))
      try {
        body
      } finally {
        kafkaBrokers.map(server => server.shutdown())
        kafkaBrokers.map(server => server.config.logDirs.map(Utils.rm(_)))
      }
    }
  }
}
