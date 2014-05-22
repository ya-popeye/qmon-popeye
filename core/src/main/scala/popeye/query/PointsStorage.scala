package popeye.query

import popeye.storage.hbase.HBaseStorage.{PointsStream, ValueNameFilterCondition}
import scala.concurrent.{ExecutionContext, Future}
import popeye.storage.hbase.{UniqueIdStorage, HBaseStorage}
import popeye.query.PointsStorage.NameType.NameType

trait PointsStorage {
  def getPoints(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition]): Future[PointsStream]

  def getSuggestions(namePrefix: String, nameType: NameType): Seq[String]
}

object PointsStorage {


  val MaxNumberOfSuggestions: Int = 10

  object NameType extends Enumeration {
    type NameType = Value
    val MetricType, AttributeNameType, AttributeValueType = Value
  }

  def createPointsStorage(pointsStorage: HBaseStorage,
                          uniqueIdStorage: UniqueIdStorage,
                          executionContext: ExecutionContext) = new PointsStorage {

    def getPoints(metric: String,
                  timeRange: (Int, Int),
                  attributes: Map[String, ValueNameFilterCondition]) =
      pointsStorage.getPoints(metric, timeRange, attributes)(executionContext)

    def getSuggestions(namePrefix: String, nameType: NameType): Seq[String] = {

      import NameType._
      val kind = nameType match {
        case MetricType => HBaseStorage.MetricKind
        case AttributeNameType => HBaseStorage.AttrNameKind
        case AttributeValueType => HBaseStorage.AttrValueKind
      }
      uniqueIdStorage.getSuggestions(kind, ???, namePrefix, MaxNumberOfSuggestions)
    }
  }
}


