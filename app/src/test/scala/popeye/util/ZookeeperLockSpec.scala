package popeye.util

import java.util.concurrent.{Executors, ExecutorService}

import akka.dispatch.ExecutionContexts
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import popeye.Logging
import popeye.test.EmbeddedZookeeper
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Await, Promise, Future}
import scala.concurrent.duration._
import scala.util.Try

class ZookeeperLockSpec extends FlatSpec with Matchers with BeforeAndAfter with Logging {
  var zookeeper: EmbeddedZookeeper = null
  var executorService: ExecutorService = null
  var executionContext: ExecutionContext = null

  before {
    executorService = {
      val threadFactory = new ThreadFactoryBuilder().setDaemon(true).build()
      Executors.newScheduledThreadPool(10, threadFactory)
    }
    executionContext = ExecutionContexts.fromExecutor(executorService)
    zookeeper = new EmbeddedZookeeper()
  }

  after {
    executorService.shutdownNow()
    zookeeper.shutdown()
  }

  behavior of "ZookeeperLock"

  it should "create lock path" in {
    val zkClientConfig = zookeeper.zkClientConfiguration
    val lock = ZookeeperLock.tryAcquireLockAndRunTask(zkClientConfig, "/parent/lock") {}
    zookeeper.newClient.readData("/parent/lock"): String
  }

  it should "not fail if lock path exists" in {
    val zkClientConfig = zookeeper.zkClientConfiguration
    ZookeeperLock.tryAcquireLockAndRunTask(zkClientConfig, "/parent/lock") {}
    ZookeeperLock.tryAcquireLockAndRunTask(zkClientConfig, "/parent/lock") {}
  }

  it should "be exclusive" in {
    val zkClientConfig = zookeeper.zkClientConfiguration
    val testCompletion = Promise[Unit]()
    val lockIsOwned = new AtomicBoolean(false)
    def testLock() = Future {
      for (_ <- 0 to 10) {
        ZookeeperLock.tryAcquireLockAndRunTask(zkClientConfig, "/lock") {
          if (lockIsOwned.compareAndSet(false, true)) {
            Thread.sleep(10)
            lockIsOwned.set(false)
          } else {
            testCompletion.failure(new AssertionError("lock is not exclusive"))
          }
        }
      }
      testCompletion.trySuccess(())
    }(executionContext)
    testLock()
    testLock()
    Await.result(testCompletion.future, 5 seconds)
  }

}
