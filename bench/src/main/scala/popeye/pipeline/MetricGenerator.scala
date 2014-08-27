package popeye.pipeline

import scala.util.Random

object MetricGenerator {
  val random = new Random()

  def randomTag = {
    def rndInt = math.abs(random.nextInt(10000) * 123841892)
    f"${rndInt}=${rndInt}"
  }

  def pointStringWithoutTags(metric: String, timestamp: Int, pointValue: Int) = {
    StringBuilder.newBuilder
      .append("put").append(' ')
      .append(metric).append(' ')
      .append(timestamp).append(' ')
      .append(pointValue)
  }

  val metrics = {
    val subNames = Seq(
      Seq("proc", "stuff", "foo", "bar"),
      Seq("cpu", "mem", "disk", "pag"),
      Seq("bytes", "ticks", "cycles", "flops"),
      Seq("10s", "30s", "1m", "5m")
    )
    generateMetricNames(subNames).toIndexedSeq
  }

  def generateMetricNames(subNames: Seq[Seq[String]]): Seq[String] = {
    if (subNames.size == 1) {
      subNames.head
    } else {
      for {
        tail <- generateMetricNames(subNames.tail)
        head <- subNames.head
      } yield f"$head.$tail"
    }
  }

  def generateTags(tags: Seq[(String, Seq[String])]): Seq[Seq[(String, String)]] = {
    val (name, values) = tags.head
    if (tags.size == 1) {
      for (value <- values) yield Seq(name -> value)
    } else {
      for {
        tailTags <- generateTags(tags.tail)
        value <- values
      } yield (name, value) +: tailTags
    }
  }
}
