package popeye.pipeline

/**
 * @author Andrey Stepachev
 */
class WorkerException(message: String, cause: Throwable) extends DispatcherException(message, cause) {
  def this(message: String) = this(message, null)
}
