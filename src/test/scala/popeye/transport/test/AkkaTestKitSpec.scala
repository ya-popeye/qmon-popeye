package popeye.transport.test

import akka.actor.ActorSystem
import org.scalatest.{FlatSpec, BeforeAndAfterAll}
import akka.testkit.{TestKit, ImplicitSender}
import org.scalatest.matchers.MustMatchers
import com.typesafe.config.ConfigFactory

abstract class AkkaTestKitSpec(name: String)
  extends TestKit(ActorSystem(name, ConfigFactory.parseString("""
  akka.loggers = ["akka.testkit.TestEventListener"]
                                                              """)))
  with FlatSpec
  with MustMatchers
  with BeforeAndAfterAll
  with ImplicitSender {

  override def afterAll() {
    system.shutdown()
  }
}
