package popeye.transport.kafka

import kafka.utils.VerifiableProperties
import kafka.serializer.Decoder
import popeye.transport.proto.Storage.Ensemble
import popeye.Logging
import com.google.protobuf.InvalidProtocolBufferException

/**
 * @author Andrey Stepachev
 */
class EnsembleDecoder(props: VerifiableProperties = null) extends Decoder[Ensemble] {

  def fromBytes(bytes: Array[Byte]): Ensemble = {
    try {
      Ensemble.parseFrom(bytes)
    } catch {
      case ex: InvalidProtocolBufferException =>
//        logger.error("Can't parse message", ex)
        return null
    }
  }
}
