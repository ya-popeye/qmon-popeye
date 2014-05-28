package popeye.util

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import popeye.test.{TestExecContext, EmbeddedZookeeper}
import org.I0Itec.zkclient.ZkClient
import scala.collection.mutable
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Await, Promise, Future}
import scala.concurrent.duration._
import scala.util.Try

class ZookeeperLockSpec extends FlatSpec with Matchers with BeforeAndAfter with TestExecContext {
  var zookeeper: EmbeddedZookeeper = null
  before {
    zookeeper = new EmbeddedZookeeper()
  }

  after {
    zookeeper.shutdown()
  }

  behavior of "ZookeeperLock"

  it should "create lock path" in {
    val zkClient = zookeeper.newClient
    val lock = ZookeeperLock.acquireLock(zkClient, "/parent/lock")
    zkClient.readData("/parent/lock"): String
  }

  it should "not fail if lock path exists" in {
    val zkClient = zookeeper.newClient
    val lock1 = ZookeeperLock.acquireLock(zkClient, "/parent/lock")
    val lock2 = ZookeeperLock.acquireLock(zkClient, "/parent/lock")
  }

  it should "be exclusive" in {
    val zkClient1 = zookeeper.newClient
    val lock1 = ZookeeperLock.acquireLock(zkClient1, "/lock")
    val zkClient2 = zookeeper.newClient
    val lock2 = ZookeeperLock.acquireLock(zkClient2, "/lock")
    val testCompletion = Promise[Unit]()
    val atomicBool = new AtomicBoolean(false)
    Future {
      Thread.sleep(100)
      if (lock1.acquired()) {
        atomicBool.set(true)
      } else {
        testCompletion.failure(new AssertionError("lock1.acquired() must return true"))
      }
      lock1.unlock()
    }
    Future {
      while(!lock2.acquired()) {
        Thread.sleep(100)
      }
      testCompletion.complete(Try {
        atomicBool.get() should be(true)
      })
    }
    Await.result(testCompletion.future, 5 seconds)
  }

}
