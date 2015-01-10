package popeye.query

import org.apache.hadoop.hbase.util.Bytes
import popeye.storage.hbase.HBaseStorage.PointsGroups
import popeye.storage.ValueNameFilterCondition
import scala.concurrent.{ExecutionContext, Future}
import popeye.storage.hbase._
import popeye.query.PointsStorage.NameType.NameType
import scala.collection.immutable.SortedSet

trait PointsStorage {
  def getPoints(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition],
                cancellation: Future[Nothing]): Future[PointsGroups]

  def getSuggestions(namePrefix: String, nameType: NameType, maxSuggestions: Int): Seq[String]
}

object PointsStorage {


  val MaxGenerations = 3

  object NameType extends Enumeration {
    type NameType = Value
    val MetricType, AttributeNameType, AttributeValueType = Value
  }

  def createPointsStorage(pointsStorage: HBaseStorage,
                          uniqueIdStorage: UniqueIdStorage,
                          timeRangeIdMapping: GenerationIdMapping,
                          executionContext: ExecutionContext) = new PointsStorage {

    def getPoints(metric: String,
                  timeRange: (Int, Int),
                  attributes: Map[String, ValueNameFilterCondition],
                  cancellation: Future[Nothing]) = {
      val groupsIterator = pointsStorage.getPoints(metric, timeRange, attributes)(executionContext)
      HBaseStorage.collectAllGroups(groupsIterator, cancellation)(executionContext)
    }

    def getSuggestions(namePrefix: String, nameType: NameType, maxSuggestions: Int): Seq[String] = {

      import NameType._
      val kind = nameType match {
        case MetricType => TsdbFormat.MetricKind
        case AttributeNameType => TsdbFormat.AttrNameKind
        case AttributeValueType => TsdbFormat.AttrValueKind
      }
      val currentTimeInSeconds = System.currentTimeMillis() / 1000
      val currentBaseTime = currentTimeInSeconds - currentTimeInSeconds % TsdbFormat.MAX_TIMESPAN
      val generationIds = timeRangeIdMapping.backwardIterator(currentBaseTime.toInt)
        .take(MaxGenerations)
        .map(_.id)
        .toSeq
      val suggestions = generationIds.flatMap {
        genId => uniqueIdStorage.getSuggestions(
          kind,
          new BytesKey(Bytes.toBytes(genId)),
          namePrefix,
          maxSuggestions
        )
      }
      SortedSet(suggestions: _*).toSeq
    }
  }
}


