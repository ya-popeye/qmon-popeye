package popeye.pipeline

import java.util.concurrent.atomic.AtomicReference

/**
 * List for rare concurrent updates
 * @author Andrey Stepachev
 */
class AtomicList[A] {
  private val x = new AtomicReference(List[A]())

  def isEmpty: Boolean = x.get.isEmpty

  def size: Int = x.get.size

  def headOption(): Option[A] = {
    while (true) {
      val oldList = x.get // get old value
      oldList match {
        case head :: tail =>
          if (x.compareAndSet(oldList, tail))
            return Some(head)
        case _ =>
          None
      }
    }
    None
  }

  def add(p: A): Unit = {
    while (true) {
      val oldList = x.get // get old value
      val newList = p :: oldList
      if (x.compareAndSet(oldList, newList))
        return
    }
  }
}

