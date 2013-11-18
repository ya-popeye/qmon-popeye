package popeye.pipeline

/**
 * @author Andrey Stepachev
 */
class DispatcherException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}
