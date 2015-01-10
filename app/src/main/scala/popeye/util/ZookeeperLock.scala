package popeye.util

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import popeye.Logging

import scala.util.Try

object ZookeeperLock extends Logging {

  def tryAcquireLockAndRunTask[A](zkClientConfig: ZkClientConfiguration,
                                  lockPath: String)
                                 (task: => A): Option[Try[A]] = {
    val ephemeralNodePath = f"$lockPath/lock"
    val zkClient = new ZkClient(
      zkClientConfig.zkConnectString,
      zkClientConfig.sessionTimeout,
      zkClientConfig.connectionTimeout,
      ZKStringSerializer)
    try {
      val createParents = true
      zkClient.createPersistent(lockPath, createParents)
      zkClient.createEphemeral(ephemeralNodePath, "")
      Some(Try(task))
    } catch {
      case e: ZkNodeExistsException =>
        info("failed to acquire lock", e)
        None
      case e: Throwable =>
        error("failed to acquire lock", e)
        None
    } finally {
      zkClient.close()
    }
  }
}