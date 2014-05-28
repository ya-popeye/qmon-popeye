package popeye

import org.slf4j.{LoggerFactory, Logger}

/**
 * @author Andrey Stepachev
 */
trait Logging {
  protected val loggerName = this.getClass.getName
  protected lazy val log: Logger = LoggerFactory.getLogger(loggerName)

  protected var logIdent = ""

  private def msgWithLogIdent(msg: String) = logIdent + msg

  protected def withTrace(body: => Unit) = {
    if (log.isTraceEnabled)
      body
  }

  protected def trace(msg: => String): Unit = {
    if (log.isTraceEnabled())
      log.trace(msgWithLogIdent(msg))
  }

  protected def trace(e: => Throwable): Any = {
    if (log.isTraceEnabled())
      log.trace(logIdent, e)
  }

  protected def trace(msg: => String, e: => Throwable) = {
    if (log.isTraceEnabled())
      log.trace(msgWithLogIdent(msg), e)
  }

  protected def trace(format: => String, arg1: => Any, arg2: => Any) = {
    if (log.isTraceEnabled())
      log.trace(msgWithLogIdent(format), arg1, arg2)
  }

  protected def swallowTrace(action: => Unit) {
    Utils.swallow(log.trace, action)
  }

  protected def withDebug(body: => Unit) = {
    if (log.isDebugEnabled)
      body
  }

  protected def debug(msg: => String): Unit = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(msg))
  }

  protected def debug(e: => Throwable): Any = {
    if (log.isDebugEnabled())
      log.debug(logIdent, e)
  }

  protected def debugThrowable(msg: => String, e: => Throwable) = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(msg), e)
  }

  protected def debug(format: => String, arg1: => Any) = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(format), arg1)
  }

  protected def debug(format: => String, arg1: => Any, arg2: => Any) = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(format), arg1, arg2)
  }

  protected def debug(format: => String, arguments: (Unit => Any)*) = {
    if (log.isDebugEnabled())
      log.debug(msgWithLogIdent(format), arguments: _*)
  }

  protected def swallowDebug(action: => Unit) {
    Utils.swallow(log.debug, action)
  }

  protected def info(msg: => String): Unit = {
    if (log.isInfoEnabled())
      log.info(msgWithLogIdent(msg))
  }

  protected def info(e: => Throwable): Any = {
    if (log.isInfoEnabled())
      log.info(logIdent, e)
  }

  protected def info(msg: => String, e: => Throwable) = {
    if (log.isInfoEnabled())
      log.info(msgWithLogIdent(msg), e)
  }

  protected def info(format: => String, arg1: => Any, arg2: => Any) = {
    log.info(msgWithLogIdent(format), arg1, arg2)
  }

  protected def info(format: => String, arguments: (Unit => Any)*) = {
    if (log.isInfoEnabled())
      log.info(msgWithLogIdent(format), arguments: _*)
  }

  protected def swallowInfo(action: => Unit) {
    Utils.swallow(log.info, action)
  }

  protected def warn(msg: => String): Unit = {
    log.warn(msgWithLogIdent(msg))
  }

  protected def warn(e: => Throwable): Any = {
    log.warn(logIdent, e)
  }

  protected def warn(msg: => String, e: => Throwable) = {
    log.warn(msgWithLogIdent(msg), e)
  }

  protected def warn(format: => String, arg1: => Any, arg2: => Any) = {
    log.warn(msgWithLogIdent(format), arg1, arg2)
  }

  protected def warn(format: => String, arguments: (Unit => Any)*) = {
    if (log.isWarnEnabled())
      log.warn(msgWithLogIdent(format), arguments: _*)
  }

  protected def swallowWarn(action: => Unit) {
    Utils.swallow(log.warn, action)
  }

  protected def swallow(action: => Unit) = swallowWarn(action)

  protected def error(msg: => String): Unit = {
    log.error(msgWithLogIdent(msg))
  }

  protected def error(e: => Throwable): Any = {
    log.error(logIdent, e)
  }

  protected def error(msg: => String, e: => Throwable) = {
    log.error(msgWithLogIdent(msg), e)
  }

  protected def error(format: => String, arg1: => Any, arg2: => Any) = {
    log.error(msgWithLogIdent(format), arg1, arg2)
  }

  protected def error(format: => String, arguments: (Unit => Any)*) = {
    if (log.isErrorEnabled())
      log.error(msgWithLogIdent(format), arguments: _*)
  }

  protected def swallowError(action: => Unit) {
    Utils.swallow(log.error, action)
  }
}
