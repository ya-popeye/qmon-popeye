package popeye.pipeline

import java.io.Closeable
import popeye.proto.{PackedPoints, Message}
import scala.concurrent.Future

/**
 * @author Andrey Stepachev
 */
trait PointsSink extends Closeable {
  def sendPoints(batchId: Long, points: Message.Point*): Future[Long]

  def sendPacked(batchId: Long, buffers: PackedPoints*): Future[Long]
}

trait PointsSinkFactory {
  def newSender(topic: String = ""): PointsSink
}
