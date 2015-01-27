package popeye

trait PointRope {

  def iterators: Iterator[Iterator[Point]]

  def iterator: Iterator[Point] = {
    val bufferedIterators: Iterator[Iterator[Point]] = iterators.toBuffer.iterator
    bufferedIterators.flatten
  }

  def filter(cond: Point => Boolean): PointRope = PointRope.fromIterator(iterator.filter(cond))

  def concat(right: PointRope) = new PointRope.CompositePointRope(Vector(this, right))

  def size: Int

  def lastOption: Option[Point]

  def asIterable = new Iterable[Point] {
    override def iterator: Iterator[Point] = PointRope.this.iterator
  }
}


object PointRope {

  private[PointRope] class CompositePointRope(pointRopes: Vector[PointRope]) extends PointRope {
    override def iterators: Iterator[Iterator[Point]] = pointRopes.iterator.flatMap(_.iterators)

    override def size: Int = pointRopes.map(_.size).sum

    override def lastOption = pointRopes.lastOption.flatMap(_.lastOption)
  }

  private[PointRope] class SinglePointArrayRope(pointArray: PointArray) extends PointRope {
    override def iterators: Iterator[Iterator[Point]] = Iterator.single(pointArray.iterator)

    override def size: Int = pointArray.size

    override def lastOption = pointArray.lastOption
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

  def lastOption =
    if (timestamps.nonEmpty) {
      Some(Point(timestamps.last, values.last))
    } else {
      None
    }


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

