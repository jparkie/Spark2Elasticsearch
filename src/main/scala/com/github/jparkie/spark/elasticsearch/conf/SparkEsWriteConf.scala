package com.github.jparkie.spark.elasticsearch.conf

import com.github.jparkie.spark.elasticsearch.util.SparkEsConfParam
import org.apache.spark.SparkConf

/**
 * Configurations for EsNativeDataFrameWriter's BulkProcessor.
 *
 * @param bulkActions The number of IndexRequests to batch in one request.
 * @param bulkSizeInMB The maximum size in MB of a batch.
 * @param concurrentRequests The number of concurrent requests in flight.
 * @param flushTimeoutInSeconds The maximum time in seconds to wait while closing a BulkProcessor.
 */
case class SparkEsWriteConf(
  bulkActions:           Int,
  bulkSizeInMB:          Int,
  concurrentRequests:    Int,
  flushTimeoutInSeconds: Long
) extends Serializable

object SparkEsWriteConf {
  val BULK_ACTIONS = SparkEsConfParam[Int](
    name = "es.batch.size.entries",
    default = 1000
  )
  val BULK_SIZE_IN_MB = SparkEsConfParam[Int](
    name = "es.batch.size.bytes",
    default = 5
  )
  val CONCURRENT_REQUESTS = SparkEsConfParam[Int](
    name = "es.batch.concurrent.request",
    default = 1
  )
  val FLUSH_TIMEOUT_IN_SECONDS = SparkEsConfParam[Long](
    name = "es.batch.flush.timeout",
    default = 10
  )

  /**
   * Extracts SparkEsTransportClientConf from a SparkConf.
   *
   * @param sparkConf A SparkConf.
   * @return A SparkEsTransportClientConf from a SparkConf.
   */
  def fromSparkConf(sparkConf: SparkConf): SparkEsWriteConf = {
    SparkEsWriteConf(
      bulkActions = sparkConf.getInt(BULK_ACTIONS.name, BULK_ACTIONS.default),
      bulkSizeInMB = sparkConf.getInt(BULK_SIZE_IN_MB.name, BULK_SIZE_IN_MB.default),
      concurrentRequests = sparkConf.getInt(CONCURRENT_REQUESTS.name, CONCURRENT_REQUESTS.default),
      flushTimeoutInSeconds = sparkConf.getLong(FLUSH_TIMEOUT_IN_SECONDS.name, FLUSH_TIMEOUT_IN_SECONDS.default)
    )
  }
}
