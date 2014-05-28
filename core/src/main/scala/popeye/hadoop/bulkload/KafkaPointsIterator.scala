package popeye.hadoop.bulkload

import kafka.consumer.SimpleConsumer
import popeye.proto.Message
import kafka.api.{FetchRequestBuilder, FetchResponse}
import popeye.javaapi.kafka.hadoop.KafkaInput
import popeye.proto.PackedPoints

class KafkaPointsIterator(kafkaConsumer: SimpleConsumer,
                          kafkaInput: KafkaInput,
                          fetchSize: Int, clientId: String) extends Iterator[Seq[Message.Point]] {

  var currentOffset: Long = kafkaInput.startOffset

  var currentIterator: Iterator[Seq[Message.Point]] = fetchNext()

  def hasNext: Boolean = {
    if (!currentIterator.hasNext && currentOffset < kafkaInput.stopOffset) {
      currentIterator = fetchNext()
    }
    currentIterator.hasNext
  }

  def next(): Seq[Message.Point] = currentIterator.next()

  private def fetchNext(): Iterator[Seq[Message.Point]] = {
    val KafkaInput(topic, partition, _, stopOffset) = kafkaInput
    val request = new FetchRequestBuilder()
      .addFetch(topic, partition, currentOffset, fetchSize)
      .clientId(clientId)
      .build()
    val response: FetchResponse = kafkaConsumer.fetch(request)
    val messageSet = response.messageSet(topic, partition)
    messageSet.iterator.filter(_.offset < stopOffset).map {
      messageAndOffset =>
        currentOffset = messageAndOffset.nextOffset
        val buffer = messageAndOffset.message.payload
        val bytes = kafka.utils.Utils.readBytes(buffer)
        PackedPoints.decodePoints(bytes)
    }
  }

  def getProgress: Float = {
    val stopOffset: Long = kafkaInput.stopOffset
    val startOffset: Long = kafkaInput.startOffset
    (currentOffset - startOffset).toFloat / (stopOffset - startOffset)
  }

  def close() = {
    kafkaConsumer.close()
  }
}
