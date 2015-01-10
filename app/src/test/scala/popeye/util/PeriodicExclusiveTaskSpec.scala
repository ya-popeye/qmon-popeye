package popeye.util

import java.util.concurrent.{ExecutorService, Executors}

import akka.dispatch.ExecutionContexts
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import scala.concurrent.{ExecutionContext, Await, Promise}
import popeye.test.EmbeddedZookeeper
import akka.actor.ActorSystem
import scala.concurrent.duration._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

class PeriodicExclusiveTaskSpec extends FlatSpec with Matchers with BeforeAndAfter {

  var executorService: ExecutorService = null
  var executionContext: ExecutionContext = null

  var zookeeper: EmbeddedZookeeper = null
  var actorSystem: ActorSystem = null
  before {
    executorService = {
      val threadFactory = new ThreadFactoryBuilder().setDaemon(true).build()
      Executors.newScheduledThreadPool(10, threadFactory)
    }
    executionContext = ExecutionContexts.fromExecutor(executorService)
    zookeeper = new EmbeddedZookeeper()
    actorSystem = ActorSystem()
  }

  after {
    executorService.shutdownNow()
    zookeeper.shutdown()
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  behavior of "PeriodicExclusiveTask"

  val lockPath = "/lock"

  it should "run task periodically" in {
    val testCompletion = Promise[Unit]()
    val runCount = new AtomicInteger(0)
    PeriodicExclusiveTask.run(
      zookeeper.zkClientConfiguration,
      lockPath,
      actorSystem.scheduler,
      executionContext,
      10 millis
    ) {
      if (runCount.getAndIncrement == 10) {
        testCompletion.success(())
      }
    }
    Await.result(testCompletion.future, 1 second)
  }

  it should "handle long tasks" in {
    val testCompletion = Promise[Unit]()
    val runCount = new AtomicInteger(0)
    PeriodicExclusiveTask.run(
      zookeeper.zkClientConfiguration,
      lockPath,
      actorSystem.scheduler,
      executionContext,
      10 millis
    ) {
      runCount.getAndIncrement match {
        case 1 =>
          Thread.sleep(500)
          testCompletion.success(())
        case 2 =>
          testCompletion.failure(new AssertionError("second task started too early"))
      }
    }
    Await.result(testCompletion.future, 1 second)
  }

  it should "run operations exclusively" in {
    val testCompletion = Promise[Unit]()
    val runCount = new AtomicInteger(0)
    val taskIsRunning = new AtomicBoolean(false)
    def startPeriodicTask() = {
      PeriodicExclusiveTask.run(
        zookeeper.zkClientConfiguration,
        lockPath,
        actorSystem.scheduler,
        executionContext,
        10 millis
      ) {
        if (taskIsRunning.compareAndSet(false, true)) {
          if (runCount.getAndIncrement == 10) {
            testCompletion.success(())
          }
        } else {
          testCompletion.failure(new AssertionError("fail"))
        }
        Thread.sleep(20)
        taskIsRunning.set(false)
      }
    }
    startPeriodicTask()
    startPeriodicTask()
    Await.result(testCompletion.future, 1 second)
  }

  it should "run operations exclusively in case of a failure" in {
    val testCompletion = Promise[Unit]()
    val runCount = new AtomicInteger(0)
    val taskIsRunning = new AtomicBoolean(false)
    def startPeriodicTask() = {
      PeriodicExclusiveTask.run(
        zookeeper.zkClientConfiguration,
        lockPath,
        actorSystem.scheduler,
        executionContext,
        10 millis
      ) {
        if (taskIsRunning.compareAndSet(false, true)) {
          if (runCount.getAndIncrement == 10) {
            testCompletion.success(())
          }
        } else {
          testCompletion.failure(new AssertionError("fail"))
        }
        Thread.sleep(20)
        taskIsRunning.set(false)
        throw new RuntimeException
      }
    }
    startPeriodicTask()
    startPeriodicTask()
    Await.result(testCompletion.future, 1 second)
  }

}
