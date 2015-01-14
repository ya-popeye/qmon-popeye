package popeye.test

import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
import akka.testkit.TestKitBase
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem

abstract class AkkaTestKitSpec(name: String) extends FlatSpec
with TestKitBase
with Matchers
with BeforeAndAfterAll {

  implicit def self = testActor

  override implicit lazy val system: ActorSystem = ActorSystem(name, ConfigFactory.parseString( """
  akka.loggers = ["akka.testkit.TestEventListener"]
                                                                                           """))

  override def afterAll() {
    system.shutdown()
  }
}
