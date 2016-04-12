package com.github.jparkie.spark.elasticsearch

import java.util.concurrent.TimeUnit

import com.github.jparkie.spark.elasticsearch.conf.SparkEsWriteConf
import com.github.jparkie.spark.elasticsearch.util.SparkEsException
import org.apache.spark.{ Logging, TaskContext }
import org.elasticsearch.action.bulk.{ BulkProcessor, BulkRequest, BulkResponse }
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.{ ByteSizeUnit, ByteSizeValue }

class SparkEsBulkWriter[T](
  esIndex:           String,
  esType:            String,
  esClient:          () => Client,
  sparkEsSerializer: SparkEsSerializer[T],
  sparkEsMapper:     SparkEsMapper[T],
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

  private[elasticsearch] def applyMappings(currentRow: T, indexRequest: IndexRequest): Unit = {
    sparkEsMapper.extractMappingId(currentRow).foreach(indexRequest.id)
    sparkEsMapper.extractMappingParent(currentRow).foreach(indexRequest.parent)
    sparkEsMapper.extractMappingVersion(currentRow).foreach(indexRequest.version)
    sparkEsMapper.extractMappingVersionType(currentRow).foreach(indexRequest.versionType)
    sparkEsMapper.extractMappingRouting(currentRow).foreach(indexRequest.routing)
    sparkEsMapper.extractMappingTTLInMillis(currentRow).foreach(indexRequest.ttl(_))
    sparkEsMapper.extractMappingTimestamp(currentRow).foreach(indexRequest.timestamp)
  }

  /**
   * Upserts T to Elasticsearch by establishing a TransportClient and BulkProcessor.
   *
   * @param taskContext The TaskContext provided by the Spark DAGScheduler.
   * @param data The set of T to persist.
   */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = logDuration { () =>
    val esBulkProcessor = createBulkProcessor()

    for (currentRow <- data) {
      val currentIndexRequest = new IndexRequest(esIndex, esType)
        .source(sparkEsSerializer.write(currentRow))

      applyMappings(currentRow, currentIndexRequest)

      val currentId = currentIndexRequest.id()
      val currentParent = currentIndexRequest.parent()
      val currentVersion = currentIndexRequest.version()
      val currentVersionType = currentIndexRequest.versionType()
      val currentRouting = currentIndexRequest.routing()

      val currentUpsertRequest = new UpdateRequest(esIndex, esType, currentId)
        .parent(currentParent)
        .version(currentVersion)
        .versionType(currentVersionType)
        .routing(currentRouting)
        .doc(currentIndexRequest)
        .docAsUpsert(true)

      esBulkProcessor.add(currentUpsertRequest)
    }

    closeBulkProcessor(esBulkProcessor)
  }
}