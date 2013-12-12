package popeye.query

import com.typesafe.config.Config
import popeye.storage.hbase.HBaseStorage
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext

trait HttpServerFactory {
  def runServer(config: Config, storage: HBaseStorage, system: ActorSystem, executionContext: ExecutionContext): Unit
}
