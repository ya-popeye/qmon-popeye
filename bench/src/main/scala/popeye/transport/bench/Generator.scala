package popeye.transport.bench

import scala.annotation.tailrec
import java.io.OutputStream

/**
 * @author Andrey Stepachev
 */
class Generator(val metricName: String, val startTime: Long, val endTime: Long, val tags: List[(String, String)]) {

  val tagsString = tags.map(p => p._1 + "=" + p._2).mkString(" ")

  def generate(output: OutputStream)(data: Stream[Int]): Unit = {
    doGenerate(data, output, startTime)
  }

  @tailrec
  private def doGenerate(data: Stream[Int], output: OutputStream, timestamp: Long): Unit = {
    if (timestamp < endTime) {
      val next = data.head
      val builder = StringBuilder.newBuilder
      builder.append("put")
        .append(" ").append(metricName)
        .append(" ").append(timestamp / 1000)
        .append(" ").append(next)
        .append(" ").append(tagsString)
        .append("\n")
      output.write(builder.result().getBytes())
      doGenerate(data.tail, output, timestamp + 1000)
    }
  }
}
