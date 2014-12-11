package popeye.inttesting

import popeye.Logging
import popeye.pipeline.kafka.KafkaQueueSizeGauge

import scala.util.{Success, Failure, Try}

object PopeyeIntTestingUtils extends Logging {
  def waitWhileKafkaQueueIsNotEmpty(kafkaQueueSizeGauge: KafkaQueueSizeGauge) = {
    var kafkaQueueSizeTry: Try[Long] = Failure(new Exception("hasn't run yet"))
    while(kafkaQueueSizeTry != Success(0l)) {
      info(s"waiting for kafka queue to be empty: $kafkaQueueSizeTry")
      Thread.sleep(1000)
      kafkaQueueSizeTry = Try(kafkaQueueSizeGauge.fetchQueueSizes.values.sum)
    }
    info("kafka queue is empty")
  }
}
