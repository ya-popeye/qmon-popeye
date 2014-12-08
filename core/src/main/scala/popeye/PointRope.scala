package popeye

trait PointRope {
  def iterator: Iterator[Point]

  def filter(cond: Point => Boolean): PointRope = PointRope.fromIterator(iterator.filter(cond))

  def concat(right: PointRope) = new PointRope.CompositePointRope(Vector(this, right))

  def size: Int
}


object PointRope {

  private[PointRope] class CompositePointRope(pointRopes: Vector[PointRope]) extends PointRope {
    override def iterator: Iterator[Point] = pointRopes.iterator.flatMap(_.iterator)

    override def size: Int = pointRopes.map(_.size).sum
  }

  private[PointRope] class SinglePointArrayRope(pointArray: PointArray) extends PointRope {
    override def iterator: Iterator[Point] = pointArray.iterator

    override def size: Int = pointArray.size
  }

  def fromIterator(points: Iterator[Point]) = {
    val pArray = PointArray.fromIterator(points)
    new SinglePointArrayRope(pArray)
  }

  def concatAll(ropes: Traversable[PointRope]) = {
    new CompositePointRope(ropes.toVector)
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

