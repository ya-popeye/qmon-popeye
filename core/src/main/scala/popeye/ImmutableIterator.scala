package popeye

import scala.concurrent.{ExecutionContext, Future}

trait ImmutableIterator[A] {

  def next: Option[(A, ImmutableIterator[A])]

  def map[B](f: A => B) = new MappedImmutableIterator(this, f)

  def foldLeft[B](init: B)(f: (B, A) => B): B = ImmutableIterator.foldLeft(this, init, f)
}

object ImmutableIterator {
  def empty[A] = new EmptyImmutableIterator[A]

  def foldLeft[A, B](iter: ImmutableIterator[A], init: B, f: (B, A) => B): B = {
    var current = iter.next
    var acc = init
    while(current != None) {
      val Some((elem, nextIter)) = current
      acc = f(acc, elem)
      current = nextIter.next
    }
    acc
  }
}

class MappedImmutableIterator[A, B](iter: ImmutableIterator[A], f: A => B) extends ImmutableIterator[B] {
  override def next: Option[(B, ImmutableIterator[B])] = {
    iter.next.map {
      case (element, nextIter) =>
        val mappedElement = f(element)
        val mappedIter = new MappedImmutableIterator(nextIter, f)
        (mappedElement, mappedIter)
    }
  }
}

class EmptyImmutableIterator[A] extends ImmutableIterator[A] {
  override def next: Option[(A, ImmutableIterator[A])] = None
}

object AsyncIterator {

  def empty[A] = new EmptyAsyncIterator[A]

  def fromImmutableIterator[A](iter: ImmutableIterator[A]): AsyncIterator[A] =
    new AsyncIterator[A] {
      override def next(implicit eCtx: ExecutionContext): Future[Option[(A, AsyncIterator[A])]] = Future {
        iter.next.map {
          case (elem, nextIter) => (elem, fromImmutableIterator(nextIter))
        }
      }
    }

  def foldLeft[A, B](iter: AsyncIterator[A],
                     init: B,
                     f: ((B, A) => B),
                     cancellation: Future[Nothing])
                    (implicit eCtx: ExecutionContext): Future[B] = {
    val foldLeftFuture = foldLeftInner(iter, init, f, cancellation)
    Future.firstCompletedOf(Seq(foldLeftFuture, cancellation))
  }

  private def foldLeftInner[A, B](iter: AsyncIterator[A],
                                  init: B,
                                  f: ((B, A) => B),
                                  cancellation: Future[Nothing])
                                 (implicit eCtx: ExecutionContext): Future[B] = {
    if (cancellation.isCompleted) {
      cancellation
    } else {
      iter.next.flatMap {
        case Some((element, nextIter)) =>
          foldLeftInner(nextIter, f(init, element), f, cancellation)
        case None =>
          Future.successful(init)
      }
    }
  }

  def unwrapFuture[A](future: Future[AsyncIterator[A]]): AsyncIterator[A] = new AsyncIterator[A] {
    override def next(implicit eCtx: ExecutionContext): Future[Option[(A, AsyncIterator[A])]] = future.flatMap(_.next)
  }

}

trait AsyncIterator[A] {
  def next(implicit eCtx: ExecutionContext): Future[Option[(A, AsyncIterator[A])]]

  def map[B](f: A => Future[B])(implicit eCtx: ExecutionContext): AsyncIterator[B] =
    new MappedAsyncIterator[A, B](this, f)
}

class MappedAsyncIterator[A, B](iter: AsyncIterator[A], f: A => Future[B]) extends AsyncIterator[B] {
  override def next(implicit eCtx: ExecutionContext): Future[Option[(B, AsyncIterator[B])]] = {
    val func = f
    val next = iter.next
    next.flatMap {
      case Some((element, nextIter)) =>
        val flatMappedElementFuture = f(element)
        flatMappedElementFuture.map {
          elem =>
            val flatMappedIter = new MappedAsyncIterator(nextIter, func)
            Some(elem, flatMappedIter)
        }
      case None => Future.successful(None)
    }
  }
}

class EmptyAsyncIterator[A] extends AsyncIterator[A] {
  override def next(implicit eCtx: ExecutionContext): Future[Option[(A, AsyncIterator[A])]] = Future.successful(None)
}


