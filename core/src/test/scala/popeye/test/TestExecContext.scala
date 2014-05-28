package popeye.test

import akka.dispatch.ExecutionContexts
import java.util.concurrent.Executors
import com.google.common.util.concurrent.ThreadFactoryBuilder

trait TestExecContext {
  implicit lazy val executionContext = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).build()
    val executorService = Executors.newScheduledThreadPool(10, threadFactory)
    ExecutionContexts.fromExecutor(executorService)
  }

  def executionContextPoolSize = 10
}
