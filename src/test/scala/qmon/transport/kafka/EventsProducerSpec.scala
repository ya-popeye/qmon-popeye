package qmon.transport.kafka

import org.scalatest.FlatSpec
import qmon.transport.test.{KafkaServerTestSpec, ZkTestSpec}
import kafka.utils.Logging
import java.util
import org.scalatest.matchers.MustMatchers

/**
 * @author Andrey Stepachev
 */
class EventsProducerSpec extends FlatSpec with KafkaServerTestSpec with Logging with MustMatchers {
  "producer" should "produce published events" in withKafkaServer() {
    val children: util.List[String] = zkClient.getChildren("/")
    logger.debug(children)
    children.size() must equal (4)

  }
}
