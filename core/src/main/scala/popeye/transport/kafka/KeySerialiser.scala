package popeye.transport.kafka

import kafka.utils.VerifiableProperties
import kafka.serializer.Encoder

/**
 * @author Andrey Stepachev
 */
class KeySerialiser(props: VerifiableProperties = null) extends Encoder[Long] {
  def toBytes(p1: Long): Array[Byte] = {
    val conv = new Array[Byte](8)
    var input = p1
    conv(7) = (input & 0xff).toByte
    input >>= 8
    conv(6) = (input & 0xff).toByte
    input >>= 8
    conv(5) = (input & 0xff).toByte
    input >>= 8
    conv(4) = (input & 0xff).toByte
    input >>= 8
    conv(3) = (input & 0xff).toByte
    input >>= 8
    conv(2) = (input & 0xff).toByte
    input >>= 8
    conv(1) = (input & 0xff).toByte
    input >>= 8
    conv(0) = input.toByte
    conv
  }

}
