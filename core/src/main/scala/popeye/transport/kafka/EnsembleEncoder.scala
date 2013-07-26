package popeye.transport.kafka

import kafka.utils.VerifiableProperties
import kafka.serializer.Encoder
import popeye.transport.proto.Storage.Ensemble

/**
 * @author Andrey Stepachev
 */
class EnsembleEncoder(props: VerifiableProperties = null) extends Encoder[Ensemble] {
  override def toBytes(value: Ensemble): Array[Byte] = value.toByteArray
}
