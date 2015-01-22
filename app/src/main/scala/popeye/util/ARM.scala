package popeye.util

import java.io.Closeable

// Automatic Resource Management
trait ARM[A] {
  def run(): A

  def compose[B](f: A => ARM[B]): ARM[B]

  def map[B](f: A => B): ARM[B] = compose(x => ARM.lift(() => f(x)))

  def flatMap[B](f: A => ARM[B]): ARM[B] = compose(f)
}

trait AutoCloseARM[A] extends ARM[A] {
  def close(x: A): Unit

  override def compose[B](f: (A) => ARM[B]): ARM[B] = {
    val self = this
    ARM.lift { () =>
      val resource = self.run()
      try {
        f(resource).run()
      } finally {
        close(resource)
      }
    }
  }
}

object ARM {

  def lift[A](runFunc: () => A): ARM[A] = new ARM[A] {
    override def run(): A = runFunc()

    override def compose[B](f: A => ARM[B]): ARM[B] = f(run())
  }

  def closableResource[R <: Closeable](res: () => R): ARM[R] = resource(res, _.close())

  def resource[R](resource: () => R, closeFunc: R => Unit): ARM[R] = new AutoCloseARM[R] {
    override def run(): R = resource()

    override def close(x: R): Unit = closeFunc(x)
  }
}