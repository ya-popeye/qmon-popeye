package popeye.util

import akka.actor.Scheduler
import scala.concurrent.duration.FiniteDuration
import org.I0Itec.zkclient.ZkClient
import popeye.Logging
import scala.concurrent.ExecutionContext

object PeriodicExclusiveTask extends Logging {
  def run(zookeeperConnection: ZkClient,
          lockPath: String,
          scheduler: Scheduler,
          executionContext: ExecutionContext,
          period: FiniteDuration)
         (task: => Unit) = {
    val zooLock = ZookeeperLock.acquireLock(zookeeperConnection, lockPath)
    scheduler.schedule(period, period) {
      if (zooLock.acquired()) try {
        task
      } catch {
        case e: Exception =>
          warn("periodic task failed", e)
      }
    }(executionContext)
  }

}
