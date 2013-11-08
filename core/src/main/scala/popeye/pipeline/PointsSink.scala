package popeye.pipeline

import popeye.transport.proto.PackedPoints
import scala.concurrent.Future

/**
 * @author Andrey Stepachev
 */
trait PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long]
}
