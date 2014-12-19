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
    info(f"task was scheduled: zk.quorum: ${zkClientConfig.zkConnectString}, " +
      f"lock path: $lockPath, period (seconds): ${period.toSeconds}")
    scheduler.schedule(period, period) {
      ZookeeperLock.tryAcquireLockAndRunTask(zkClientConfig, lockPath) {
        try {
          info(f"starting task")
          task
          info(f"task finished")
        } catch {
          case e: Exception =>
            error("periodic task failed", e)
        }
      }
    }(executionContext)
  }
}
