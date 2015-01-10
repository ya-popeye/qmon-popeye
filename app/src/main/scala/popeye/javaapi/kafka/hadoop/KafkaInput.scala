package popeye.javaapi.kafka.hadoop

import scala.collection.JavaConverters._

object KafkaInput {
  def parseStringAsInputJavaList(inputsString: String): java.util.List[KafkaInput] = {
    val inputStrings = inputsString.split(";").toSeq.filter(_.nonEmpty)
    inputStrings.map(parseStringAsInput).asJava
  }

  def parseStringAsInput(inputString: String) = {
    val tokens = inputString.split(" ")
    val topic = tokens(0)
    val partition = tokens(1).toInt
    val startOffset = tokens(2).toLong
    val stopOffset = tokens(3).toLong
    KafkaInput(topic, partition, startOffset, stopOffset)
  }

  def renderInputsString(inputs: Seq[KafkaInput]) = {
    inputs.map(renderInputString).mkString(";")
  }

  def renderInputString(input: KafkaInput) = f"${input.topic} ${input.partition} ${input.startOffset} ${input.stopOffset}"

}

case class KafkaInput(topic: String, partition: Int, startOffset: Long, stopOffset: Long) {
  require(startOffset <= stopOffset)

  def isEmpty = startOffset == stopOffset
}