package popeye.transport.kafka

/**
 * @author Andrey Stepachev
 */
private object KafkaPointsConsumerProto {
  case class NextChunk()

  case class FailedChunk()

  case class CompletedChunk()
}
