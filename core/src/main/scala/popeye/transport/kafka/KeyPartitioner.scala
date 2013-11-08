package popeye.transport.kafka

import kafka.utils.VerifiableProperties
import kafka.producer.Partitioner

/**
 * @author Andrey Stepachev
 */
class KeyPartitioner(props: VerifiableProperties = null) extends Partitioner[Long] {
  def partition(data: Long, numPartitions: Int): Int = (data % numPartitions).toInt
}
