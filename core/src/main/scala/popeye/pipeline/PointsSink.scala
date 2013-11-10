package popeye.pipeline

import popeye.proto.PackedPoints
import scala.concurrent.Future
import java.io.Closeable

/**
 * @author Andrey Stepachev
 */
trait PointsSink extends Closeable {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long]
}

trait PointsSinkFactory {
  def newPointsSink(): PointsSink
}
