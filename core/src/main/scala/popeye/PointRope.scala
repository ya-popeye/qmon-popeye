package popeye

class PointRope(protected val pointArrays: Vector[PointArray]) {
  def iterator: Iterator[Point] = pointArrays.iterator.map(_.iterator).flatten

  def filter(cond: Point => Boolean): PointRope = PointRope.fromIterator(iterator.filter(cond))

  def concat(right: PointRope) = new PointRope(pointArrays ++ right.pointArrays)

  def size = pointArrays.iterator.map(_.size).sum

  override def toString: String = pointArrays.mkString("PointRope(", ", ", ")")
}

object PointRope {

  def fromIterator(points: Iterator[Point]) = {
    val pArray = PointArray.fromIterator(points)
    new PointRope(Vector(pArray))
  }

  def concatAll(ropes: Traversable[PointRope]) = {
    val arrays = ropes.map(_.pointArrays).flatten
    new PointRope(arrays.toVector)
  }

  def fromPoints(points: Point*) = fromIterator(points.iterator)

  def apply(points: Point*): PointRope = fromPoints(points: _*)

}

class PointArray(protected val timestamps: Array[Int],
                 protected val values: Array[Double]) {

  require(
    timestamps.length == values.length,
    f"timestamps.length != values.length: ${ timestamps.length } != ${ values.length }"
  )

  def iterator: Iterator[Point] = {
    val indexes = 0 until timestamps.length
    indexes.iterator.map {
      i => Point(timestamps(i), values(i))
    }
  }

  def size = timestamps.length

  override def toString: String = iterator.mkString("PointArray(", ", ", ")")
}

object PointArray {

  def fromIterator(iterator: Iterator[Point]): PointArray = {
    val points = iterator.toBuffer
    val pointsCount = points.size
    val timestamps = Array.ofDim[Int](pointsCount)
    val values = Array.ofDim[Double](pointsCount)
    for (i <- 0 until pointsCount) {
      val Point(timestamp, value) = points(i)
      timestamps(i) = timestamp
      values(i) = value
    }
    new PointArray(timestamps, values)
  }

}

