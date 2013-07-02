package qmon.transport.test

import kafka.server.{KafkaRequestHandler, KafkaServer, KafkaConfig}
import kafka.common.KafkaException
import kafka.utils.{Utils, TestUtils}
import org.apache.log4j.Logger

/**
 * @author Andrey Stepachev
 */
trait KafkaServerTestSpec extends ZkTestSpec {
  val port = TestUtils.choosePort()
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props)
  val configs = List(config)
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
  var servers: List[KafkaServer] = null

  def withKafkaServer()(body: => Unit) {
    withZk() {
      if (configs.size <= 0)
        throw new KafkaException("Must suply at least one server config.")
      servers = configs.map(TestUtils.createServer(_))
      try {
        body
      } finally {
        servers.map(server => server.shutdown())
        servers.map(server => server.config.logDirs.map(Utils.rm(_)))
      }
    }
  }
}
