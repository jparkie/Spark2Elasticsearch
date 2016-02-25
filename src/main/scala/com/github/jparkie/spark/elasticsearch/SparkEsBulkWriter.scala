package com.github.jparkie.spark.elasticsearch

import java.util.concurrent.TimeUnit

import com.github.jparkie.spark.elasticsearch.conf.SparkEsWriteConf
import com.github.jparkie.spark.elasticsearch.util.SparkEsException
import org.apache.spark.{ Logging, TaskContext }
import org.elasticsearch.action.bulk.{ BulkProcessor, BulkRequest, BulkResponse }
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.{ ByteSizeUnit, ByteSizeValue }

class SparkEsBulkWriter[T](
  esIndex:           String,
  esType:            String,
  esClient:          () => Client,
  sparkEsSerializer: () => SparkEsSerializer[T],
  sparkEsMapper:     () => SparkEsMapper[T],
  sparkEsWriteConf:  SparkEsWriteConf
) extends Serializable with Logging {
  /**
   * Logs the executionId, number of requests, size, and latency of flushes.
   */
  class SparkEsBulkProcessorListener() extends BulkProcessor.Listener {
    override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
      logInfo(s"For executionId ($executionId), executing ${request.numberOfActions()} actions of estimate size ${request.estimatedSizeInBytes()} in bytes.")
    }

    override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
      logInfo(s"For executionId ($executionId), executed ${request.numberOfActions()} in ${response.getTookInMillis} milliseconds.")

      if (response.hasFailures) {
        throw new SparkEsException(response.buildFailureMessage())
      }
    }

    override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
      logError(s"For executionId ($executionId), BulkRequest failed.", failure)

      throw new SparkEsException(failure.getMessage, failure)
    }
  }

  private lazy val esSerializer = sparkEsSerializer()

  private lazy val esMapper = sparkEsMapper()

  private[elasticsearch] def logDuration(closure: () => Unit): Unit = {
    val localStartTime = System.nanoTime()

    closure()

    val localEndTime = System.nanoTime()

    val differenceTime = localEndTime - localStartTime
    logInfo(s"Elasticsearch Task completed in ${TimeUnit.MILLISECONDS.convert(differenceTime, TimeUnit.NANOSECONDS)} milliseconds.")
  }

  private[elasticsearch] def createBulkProcessor(): BulkProcessor = {
    val esBulkProcessorListener = new SparkEsBulkProcessorListener()
    val esBulkProcessor = BulkProcessor.builder(esClient(), esBulkProcessorListener)
      .setBulkActions(sparkEsWriteConf.bulkActions)
      .setBulkSize(new ByteSizeValue(sparkEsWriteConf.bulkSizeInMB, ByteSizeUnit.MB))
      .setConcurrentRequests(sparkEsWriteConf.concurrentRequests)
      .build()

    esBulkProcessor
  }

  private[elasticsearch] def closeBulkProcessor(bulkProcessor: BulkProcessor): Unit = {
    val isClosed = bulkProcessor.awaitClose(sparkEsWriteConf.flushTimeoutInSeconds, TimeUnit.SECONDS)
    if (isClosed) {
      logInfo("Closed Elasticsearch Bulk Processor.")
    } else {
      logError("Elasticsearch Bulk Processor failed to close.")
    }
  }

  /**
   * Writes T to Elasticsearch by establishing a TransportClient and BulkProcessor.
   *
   * @param taskContext The TaskContext provided by the Spark DAGScheduler.
   * @param data The set of T to persist.
   */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = logDuration { () =>
    val esBulkProcessor = createBulkProcessor()

    for (currentRow <- data) {
      val currentIndexRequest = new IndexRequest(esIndex, esType) source {
        esSerializer.write(currentRow)
      }

      esMapper.extractMappingId(currentRow).foreach(currentIndexRequest.id)
      esMapper.extractMappingParent(currentRow).foreach(currentIndexRequest.parent)
      esMapper.extractMappingVersion(currentRow).foreach(currentIndexRequest.version)
      esMapper.extractMappingVersionType(currentRow).foreach(currentIndexRequest.versionType)
      esMapper.extractMappingRouting(currentRow).foreach(currentIndexRequest.routing)
      esMapper.extractMappingTTLInMillis(currentRow).foreach(currentIndexRequest.ttl(_))
      esMapper.extractMappingTimestamp(currentRow).foreach(currentIndexRequest.timestamp)

      esBulkProcessor.add(currentIndexRequest)
    }

    closeBulkProcessor(esBulkProcessor)
  }
}