package popeye.inttesting

import popeye.clients.TsPoint

object TestDataUtils {

  def createSinTsPoints(metric: String,
                        timestamps: Seq[Int],
                        periods: Seq[Int],
                        amps: Seq[Int],
                        shifts: Seq[Int],
                        shardTag: (String, String)) = {
    for {
      period <- periods
      amp <- amps
      shift <- shifts
      timestamp <- timestamps
    } yield {
      val x = ((timestamp + shift) % period).toDouble / period * 2 * math.Pi
      val value = math.sin(x).toFloat * amp
      val tags = Map("period" -> period, "amp" -> amp, "shift" -> shift).mapValues(_.toString) + shardTag
      TsPoint(metric, timestamp, Right(value), tags)
    }
  }
}
