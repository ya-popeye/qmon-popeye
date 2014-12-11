package popeye.util

import akka.actor.Scheduler
import scala.concurrent.duration.FiniteDuration
import org.I0Itec.zkclient.ZkClient
import popeye.Logging
import scala.concurrent.ExecutionContext

object PeriodicExclusiveTask extends Logging {
  def run(zkClientConfig: ZkClientConfiguration,
          lockPath: String,
          scheduler: Scheduler,
          executionContext: ExecutionContext,
          period: FiniteDuration)
         (task: => Unit) = {
    scheduler.schedule(period, period) {
      ZookeeperLock.tryAcquireLockAndRunTask(zkClientConfig, lockPath) {
        try {
          task
        } catch {
          case e: Exception =>
            warn("periodic task failed", e)
        }
      }
    }(executionContext)
  }
}
