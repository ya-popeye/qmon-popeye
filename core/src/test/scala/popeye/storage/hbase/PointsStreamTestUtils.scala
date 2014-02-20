package popeye.storage.hbase

import popeye.storage.hbase.HBaseStorage.PointsStream
import scala.concurrent.Future

object PointsStreamTestUtils {
  def createStream(groupsList: List[HBaseStorage.PointsGroups]) = {
    def streamOption(groupsList: List[HBaseStorage.PointsGroups]): Option[PointsStream] = {
      groupsList.headOption.map {
        head =>
          val next = streamOption(groupsList.tail).map {
            stream => () => Future.successful(stream)
          }
          PointsStream(head, next)
      }
    }
    streamOption(groupsList).getOrElse(throw new RuntimeException("empty groups list"))
  }
}
