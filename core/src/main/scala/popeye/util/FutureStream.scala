package popeye.util

import akka.actor.Scheduler

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Promise, ExecutionContext, Future}

object FutureStream {
  def fromItems[A](items: A*): FutureStream[A] = {
    val nextStreamOption =
      if (items.tail.nonEmpty) {
        Some(FutureStream.fromItems(items.tail: _*))
      } else {
        None
      }
    FutureStream(Future.successful(items.head), () => Future.successful(nextStreamOption))
  }
}

case class FutureStream[A](head: Future[A], tailFuture: () => Future[Option[FutureStream[A]]]) {
  def reduce(op: (Future[A], Future[A]) => Future[A])(implicit eCtx: ExecutionContext): Future[A] = {
    tailFuture().flatMap {
      case Some(tail) =>
        tail.foldLeft(head)(op)
      case None => head
    }
  }

  def reduceElements(op: (A, A) => A)(implicit eCtx: ExecutionContext): Future[A] = {
    reduce((left, right) => (left zip right).map { case (a, b) => op(a, b) })
  }

  def foldLeft[B](z: Future[B])
                 (op: (Future[B], Future[A]) => Future[B])
                 (implicit eCtx: ExecutionContext): Future[B] = {
    tailFuture().flatMap {
      case Some(tail) =>
        val nextZ = op(z, head)
        tail.foldLeft(nextZ)(op)
      case None => op(z, head)
    }
  }

  def map[B](f: Future[A] => Future[B])(implicit eCtx: ExecutionContext): FutureStream[B] = {
    val newHead = f(head)
    val newTail = () => tailFuture().map {
      tailOption => tailOption.map(tail => tail.map(f))
    }
    FutureStream(newHead, newTail)
  }

  def flatMapElements[B](f: A => Future[B])(implicit eCtx: ExecutionContext): FutureStream[B] = {
    val newHead = head.flatMap(f)
    val newTail = () => tailFuture().map {
      tailOption => tailOption.map(tail => tail.flatMapElements(f))
    }
    FutureStream(newHead, newTail)
  }

  def mapElements[B](f: A => B)(implicit eCtx: ExecutionContext): FutureStream[B] = {
    val newHead = head.map(f)
    val newTail = () => tailFuture().map {
      tailOption => tailOption.map(tail => tail.mapElements(f))
    }
    FutureStream(newHead, newTail)
  }

  def withTimeout(scheduler: Scheduler, timeout: FiniteDuration)(implicit eCtx: ExecutionContext): FutureStream[A] = {
    val promise = Promise[Nothing]()
    scheduler.scheduleOnce(timeout) {
      promise.failure(new RuntimeException("stream timeout"))
    }
    cancellableStream(promise.future)
  }

  private def cancellableStream(cFuture: Future[Nothing])
                               (implicit eCtx: ExecutionContext): FutureStream[A] = {
    def cancellableFuture[T](future: => Future[T]) = {
      if (cFuture.isCompleted) {
        cFuture
      } else {
        Future.firstCompletedOf(Seq(future, cFuture))
      }
    }
    val cancellableTail = () => {
      val future = tailFuture().map {
        tailOption => tailOption.map(tail => tail.cancellableStream(cFuture))
      }
      cancellableFuture(future)
    }
    FutureStream(
      cancellableFuture(head),
      cancellableTail
    )
  }
}

