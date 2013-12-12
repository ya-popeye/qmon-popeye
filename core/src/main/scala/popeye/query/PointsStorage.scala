package popeye.query

import popeye.storage.hbase.HBaseStorage.{PointsStream, ValueNameFilterCondition}
import scala.concurrent.{ExecutionContext, Future}
import popeye.storage.hbase.HBaseStorage

trait PointsStorage {
  def getPoints(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition]): Future[PointsStream]
}

object PointsStorage {
  def fromHBaseStorage(storage: HBaseStorage, executionContext: ExecutionContext) = new PointsStorage {
    def getPoints(metric: String,
                  timeRange: (Int, Int),
                  attributes: Map[String, ValueNameFilterCondition]) =
      storage.getPoints(metric, timeRange, attributes)(executionContext)
  }
}


