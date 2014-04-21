package popeye.util

object KafkaUtilTest {
  def main(args: Array[String]) {
    val offsets = KafkaUtils.fetchLatestOffsets(Seq("localhost" -> 9091, "localhost" -> 9092), "popeye-points", 0 until 10)
    println(offsets)
  }
}
