package popeye.storage.hbase

import com.typesafe.config.Config
import popeye.transport.proto.{PackedPoints}
import scala.concurrent.{ExecutionContext, Future}
import popeye.pipeline.PointsSink

/**
 * @author Andrey Stepachev
 */
class HBasePointsSink(config: Config, storage: PointsStorage)(implicit eCtx: ExecutionContext) extends PointsSink {
  def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] = {
    storage.writePoints(points)(eCtx).mapTo[Long]
  }
}
