package popeye

import org.slf4j.{LoggerFactory, Logger}

/**
 * @author Andrey Stepachev
 */
trait Logging {
  val loggerName = this.getClass.getName
  lazy val log: Logger = LoggerFactory.getLogger(loggerName)

  protected var logIdent = ""

  private def msgWithLogIdent(msg: String) = logIdent + msg

  def withTrace(body: => Unit) = {
    if (log.isTraceEnabled)
      body
  }

  def trace(msg: => String): Unit = {
    if (log.isTraceEnabled())
      log.trace(msgWithLogIdent(msg))
  }

  def trace(e: => Throwable): Any = {
    if (log.isTraceEnabled())
      log.trace(logIdent, e)
  }

  def trace(msg: => String, e: => Throwable) = {
    if (log.isTraceEnabled())
      log.trace(msgWithLogIdent(msg), e)
  }

  def trace(format: => String, arg1: => Any, arg2: => Any) = {
    if (log.isTraceEnabled())
      log.trace(msgWithLogIdent(format), arg1, arg2)
  }

  def swallowTrace(action: => Unit) {
    Utils.swallow(log.trace, action)
  }

  def withDebug(body: => Unit) = {
    if (log.isDebugEnabled)
      body
  }

  def debug(msg: => String): Unit = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(msg))
  }

  def debug(e: => Throwable): Any = {
    if (log.isDebugEnabled())
      log.debug(logIdent, e)
  }

  def debugThrowable(msg: => String, e: => Throwable) = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(msg), e)
  }

  def debug(format: => String, arg1: => Any) = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(format), arg1)
  }

  def debug(format: => String, arg1: => Any, arg2: => Any) = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(format), arg1, arg2)
  }

  def debug(format: => String, arguments: (Unit => Any)*) = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(format), arguments: _*)
  }

  def swallowDebug(action: => Unit) {
    Utils.swallow(log.debug, action)
  }

  def info(msg: => String): Unit = {
    if (log.isInfoEnabled())
      log.info(msgWithLogIdent(msg))
  }

  def info(e: => Throwable): Any = {
    if (log.isInfoEnabled())
      log.info(logIdent, e)
  }

  def info(msg: => String, e: => Throwable) = {
    if (log.isInfoEnabled())
      log.info(msgWithLogIdent(msg), e)
  }

  def info(format: => String, arg1: => Any, arg2: => Any) = {
    log.info(msgWithLogIdent(format), arg1, arg2)
  }

  def info(format: => String, arguments: (Unit => Any)*) = {
    if (log.isInfoEnabled())
      log.info(msgWithLogIdent(format), arguments: _*)
  }

  def swallowInfo(action: => Unit) {
    Utils.swallow(log.info, action)
  }

  def warn(msg: => String): Unit = {
    log.warn(msgWithLogIdent(msg))
  }

  def warn(e: => Throwable): Any = {
    log.warn(logIdent, e)
  }

  def warn(msg: => String, e: => Throwable) = {
    log.warn(msgWithLogIdent(msg), e)
  }

  def warn(format: => String, arg1: => Any, arg2: => Any) = {
    log.warn(msgWithLogIdent(format), arg1, arg2)
  }

  def warn(format: => String, arguments: (Unit => Any)*) = {
    if (log.isWarnEnabled())
      log.warn(msgWithLogIdent(format), arguments: _*)
  }

  def swallowWarn(action: => Unit) {
    Utils.swallow(log.warn, action)
  }

  def swallow(action: => Unit) = swallowWarn(action)

  def error(msg: => String): Unit = {
    log.error(msgWithLogIdent(msg))
  }

  def error(e: => Throwable): Any = {
    log.error(logIdent, e)
  }

  def error(msg: => String, e: => Throwable) = {
    log.error(msgWithLogIdent(msg), e)
  }

  def error(format: => String, arg1: => Any, arg2: => Any) = {
    log.error(msgWithLogIdent(format), arg1, arg2)
  }

  def error(format: => String, arguments: (Unit => Any)*) = {
    if (log.isErrorEnabled())
      log.error(msgWithLogIdent(format), arguments: _*)
  }

  def swallowError(action: => Unit) {
    Utils.swallow(log.error, action)
  }
}
