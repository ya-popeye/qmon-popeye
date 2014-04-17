package popeye.util

import org.I0Itec.zkclient.ZkClient
import scala.collection.JavaConverters._

object ZookeeperLock {
  val numberOfDigitsInSequenceString: Int = 10

  def acquireLock(zkClient: ZkClient, lockPath: String) = {
    val createParents = true
    zkClient.createPersistent(lockPath, createParents)
    val ephNodePath = zkClient.createEphemeralSequential(f"$lockPath/lock", "")
    new ZookeeperLock(zkClient, lockPath, ephNodePath)
  }
}

class ZookeeperLock(zkClient: ZkClient, lockPath: String, ephemeralNodePath: String) {

  def acquired(): Boolean = synchronized {
    val lockNodes = zkClient.getChildren(lockPath).asScala
    val lockSequenceIndex = getSequenceIndex(ephemeralNodePath)

    val thisNodeIndexIsNotLowest = lockNodes.map(getSequenceIndex).exists {
      otherSeqNumber =>
        otherSeqNumber < lockSequenceIndex
    }

    !thisNodeIndexIsNotLowest
  }

  def unlock() = {
    zkClient.delete(ephemeralNodePath)
  }

  private def getSequenceIndex(path: String): Long = {
    path.drop(path.length - ZookeeperLock.numberOfDigitsInSequenceString).toLong
  }
}
