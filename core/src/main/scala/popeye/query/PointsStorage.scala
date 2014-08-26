package popeye.query

import akka.actor.Scheduler
import org.apache.hadoop.hbase.util.Bytes
import popeye.storage.hbase.HBaseStorage.{ListPointsStream, PointsStream, ValueNameFilterCondition}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import popeye.storage.hbase._
import popeye.query.PointsStorage.NameType.NameType
import scala.collection.immutable.SortedSet

trait PointsStorage {
  def getPoints(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition]): Future[PointsStream]

  def getListPoints(metric: String,
                    timeRange: (Int, Int),
                    attributes: Map[String, ValueNameFilterCondition]): Future[ListPointsStream]

  def getSuggestions(namePrefix: String, nameType: NameType): Seq[String]
}

object PointsStorage {


  val MaxNumberOfSuggestions: Int = 10
  val MaxGenerations = 3

  object NameType extends Enumeration {
    type NameType = Value
    val MetricType, AttributeNameType, AttributeValueType = Value
  }

  def createPointsStorage(pointsStorage: HBaseStorage,
                          uniqueIdStorage: UniqueIdStorage,
                          timeRangeIdMapping: GenerationIdMapping,
                          fetchTimeout: FiniteDuration,
                          scheduler: Scheduler,
                          executionContext: ExecutionContext) = new PointsStorage {

    private implicit val exct = executionContext
    def getPoints(metric: String,
                  timeRange: (Int, Int),
                  attributes: Map[String, ValueNameFilterCondition]) =
      pointsStorage
        .getPoints(metric, timeRange, attributes)
        .map(_.withTimeout(scheduler, fetchTimeout))

    def getListPoints(metric: String,
                      timeRange: (Int, Int),
                      attributes: Map[String, ValueNameFilterCondition]) =
      pointsStorage
        .getListPoints(metric, timeRange, attributes)
        .map(_.withTimeout(scheduler, fetchTimeout))

    def getSuggestions(namePrefix: String, nameType: NameType): Seq[String] = {

      import NameType._
      val kind = nameType match {
        case MetricType => HBaseStorage.MetricKind
        case AttributeNameType => HBaseStorage.AttrNameKind
        case AttributeValueType => HBaseStorage.AttrValueKind
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
          MaxNumberOfSuggestions
        )
      }
      SortedSet(suggestions: _*).toSeq
    }
  }
}


