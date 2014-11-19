package popeye.bench

object BenchUtils {

  case class BenchResult(minTime: Double, medianTime: Double)

  def bench(samples: Int, iterations: Int)(body: => Unit) = {
    val execTimes = List.fill(samples) {
      val startTime = System.currentTimeMillis()
      for (_ <- 0 until iterations) {
        body
      }
      System.currentTimeMillis() - startTime
    }.sorted.map(t => t.toDouble / iterations)
    val min = execTimes.head
    val median = execTimes(execTimes.size / 2)
    BenchResult(min, median)
  }
}
