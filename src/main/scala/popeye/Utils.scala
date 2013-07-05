package popeye

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio._


/**
 * General helper functions!
 *
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc.
 *
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
object Utils extends Logging {

  /**
   * Wrap the given function in a java.lang.Runnable
   * @param fun A function
   * @return A Runnable that just executes the function
   */
  def runnable(fun: () => Unit): Runnable =
    new Runnable() {
      def run() = fun()
    }

  /**
   * Wrap the given function in a java.lang.Runnable that logs any errors encountered
   * @param fun A function
   * @return A Runnable that just executes the function
   */
  def loggedRunnable(fun: () => Unit, name: String): Runnable =
    new Runnable() {
      def run() = {
        Thread.currentThread().setName(name)
        try {
          fun()
        }
        catch {
          case t: Throwable =>
            // log any error and the stack trace
            error("error in loggedRunnable", t)
        }
      }
    }

  /**
   * Create a daemon thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  def daemonThread(runnable: Runnable): Thread =
    newThread(runnable, true)

  /**
   * Create a daemon thread
   * @param name The name of the thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  def daemonThread(name: String, runnable: Runnable): Thread =
    newThread(name, runnable, true)

  /**
   * Create a daemon thread
   * @param name The name of the thread
   * @param fun The runction to execute in the thread
   * @return The unstarted thread
   */
  def daemonThread(name: String, fun: () => Unit): Thread =
    daemonThread(name, runnable(fun))

  /**
   * Create a new thread
   * @param name The name of the thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  def newThread(name: String, runnable: Runnable, daemon: Boolean): Thread = {
    val thread = new Thread(runnable, name)
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        error("Uncaught exception in thread '" + t.getName + "':", e)
      }
    })
    thread
  }

  /**
   * Create a new thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  def newThread(runnable: Runnable, daemon: Boolean): Thread = {
    val thread = new Thread(runnable)
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        error("Uncaught exception in thread '" + t.getName + "':", e)
      }
    })
    thread
  }

  /**
   * Read the given byte buffer into a byte array
   */
  def readBytes(buffer: ByteBuffer): Array[Byte] = readBytes(buffer, 0, buffer.limit)

  /**
   * Read a byte array from the given offset and size in the buffer
   */
  def readBytes(buffer: ByteBuffer, offset: Int, size: Int): Array[Byte] = {
    val dest = new Array[Byte](size)
    if (buffer.hasArray) {
      System.arraycopy(buffer.array, buffer.arrayOffset() + offset, dest, 0, size)
    } else {
      buffer.mark()
      buffer.get(dest)
      buffer.reset()
    }
    dest
  }

  /**
   * Do the given action and log any exceptions thrown without rethrowing them
   * @param log The log method to use for logging. E.g. logger.warn
   * @param action The action to execute
   */
  def swallow(log: (String, Throwable) => Unit, action: => Unit) = {
    try {
      action
    } catch {
      case e: Throwable => log(e.getMessage(), e)
    }
  }
}
