package popeye

case class ListPoint(timestamp: Int, value: Either[Seq[Long], Seq[Float]]) {
  def isFloatList = value.isRight

  def getFloatListValue = value.right.get

  def getLongListValue = value.left.get

  def doubleListValue = {
    if (isFloatList) {
      getFloatListValue.map(_.toDouble)
    } else {
      getLongListValue.map(_.toDouble)
    }
  }
}