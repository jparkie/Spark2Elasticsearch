package com.github.jparkie.spark.elasticsearch.util

/**
 * General exceptions captured by EsNativeDataFrameBulkProcessorListener.
 *
 * @param message the detail message (which is saved for later retrieval
 *                by the { @link #getMessage()} method).
 * @param cause the cause (which is saved for later retrieval by the
 *              { @link #getCause()} method).  (A <tt>null</tt> value is
 *              permitted, and indicates that the cause is nonexistent or
 *              unknown.)
 */
class SparkEsException(message: String, cause: Throwable) extends RuntimeException(message, cause) with Serializable {
  def this() = this(null, null)
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)
}