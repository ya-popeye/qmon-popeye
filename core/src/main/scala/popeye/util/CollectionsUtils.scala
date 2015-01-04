package popeye.util

object CollectionsUtils {

  def uniqIterable[A](iterable: Iterable[A], isEqual: (A, A) => Boolean): Iterable[A] = {
    new Iterable[A] {
      override def iterator: Iterator[A] = uniqIterator(iterable.iterator, isEqual)
    }
  }

  def uniqIterator[A](iterator: Iterator[A], isEqual: (A, A) => Boolean): Iterator[A] = {
    if (iterator.isEmpty) {
      Iterator.empty
    } else {
      val firstElem = iterator.next()
      new Iterator[A] {
        var nextElem = firstElem
        var hasNextElem = true

        override def hasNext: Boolean = hasNextElem

        override def next(): A = {
          val currentElement = nextElem
          while(iterator.hasNext) {
            nextElem = iterator.next()
            if (!isEqual(currentElement, nextElem)) {
              return currentElement
            }
          }
          hasNextElem = false
          currentElement
        }
      }
    }
  }
}
