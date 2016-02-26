package com.github.jparkie.spark.elasticsearch.sql

import com.github.jparkie.spark.elasticsearch.SparkEsBulkWriter
import com.github.jparkie.spark.elasticsearch.conf.{ SparkEsMapperConf, SparkEsTransportClientConf, SparkEsWriteConf }
import com.github.jparkie.spark.elasticsearch.transport.SparkEsTransportClientManager
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Extension of DataFrame with 'bulkLoadToEs()' function.
 *
 * @param dataFrame The DataFrame to lift into extension.
 */
class SparkEsDataFrameFunctions(dataFrame: DataFrame) extends Serializable {
  /**
   * SparkContext to schedule SparkEsWriter Tasks.
   */
  private[sql] val sparkContext = dataFrame.sqlContext.sparkContext

  /**
   * Saves DataFrame into Elasticsearch with the Java API utilizing a TransportClient.
   *
   * @param esIndex Index of DataFrame in Elasticsearch.
   * @param esType Type of DataFrame in Elasticsearch.
   * @param sparkEsTransportClientConf Configurations for the TransportClient.
   * @param sparkEsMapperConf Configurations for IndexRequest.
   * @param sparkEsWriteConf Configurations for the BulkProcessor.
   *                         Empty by default.
   */
  def bulkLoadToEs(
    esIndex:                    String,
    esType:                     String,
    sparkEsTransportClientConf: SparkEsTransportClientConf = SparkEsTransportClientConf.fromSparkConf(sparkContext.getConf),
    sparkEsMapperConf:          SparkEsMapperConf          = SparkEsMapperConf.fromSparkConf(sparkContext.getConf),
    sparkEsWriteConf:           SparkEsWriteConf           = SparkEsWriteConf.fromSparkConf(sparkContext.getConf)
  )(implicit sparkEsTransportClientManager: SparkEsTransportClientManager = sparkEsTransportClientManager): Unit = {
    val sparkEsWriter = new SparkEsBulkWriter[Row](
      esIndex = esIndex,
      esType = esType,
      esClient = () => sparkEsTransportClientManager.getTransportClient(sparkEsTransportClientConf),
      sparkEsSerializer = new SparkEsDataFrameSerializer(dataFrame.schema),
      sparkEsMapper = new SparkEsDataFrameMapper(sparkEsMapperConf),
      sparkEsWriteConf = sparkEsWriteConf
    )

    sparkContext.runJob(dataFrame.rdd, sparkEsWriter.write _)
  }
}
