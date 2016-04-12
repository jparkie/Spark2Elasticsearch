package com.github.jparkie.spark.elasticsearch

import com.github.jparkie.spark.elasticsearch.conf.{ SparkEsMapperConf, SparkEsWriteConf }
import com.github.jparkie.spark.elasticsearch.sql.{ SparkEsDataFrameMapper, SparkEsDataFrameSerializer }
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.types.{ LongType, StringType, StructField, StructType }
import org.apache.spark.sql.{ Row, SQLContext }
import org.scalatest.{ MustMatchers, WordSpec }

class SparkEsBulkWriterSpec extends WordSpec with MustMatchers with SharedSparkContext {
  val esServer = new ElasticSearchServer()

  override def beforeAll(): Unit = {
    super.beforeAll()

    esServer.start()
  }

  override def afterAll(): Unit = {
    esServer.stop()

    super.afterAll()
  }

  "SparkEsBulkWriter" must {
    "execute write() successfully" in {
      esServer.createAndWaitForIndex("test_index")

      val sqlContext = new SQLContext(sc)

      val inputSparkEsWriteConf = SparkEsWriteConf(
        bulkActions = 10,
        bulkSizeInMB = 1,
        concurrentRequests = 0,
        flushTimeoutInSeconds = 1
      )
      val inputMapperConf = SparkEsMapperConf(
        esMappingId = Some("id"),
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )
      val inputSchema = StructType(
        Array(
          StructField("id", StringType, true),
          StructField("parent", StringType, true),
          StructField("version", LongType, true),
          StructField("routing", StringType, true),
          StructField("ttl", LongType, true),
          StructField("timestamp", StringType, true),
          StructField("value", LongType, true)
        )
      )
      val inputData = sc.parallelize {
        Array(
          Row("TEST_ID_1", "TEST_PARENT_1", 1L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 1L),
          Row("TEST_ID_1", "TEST_PARENT_2", 2L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 2L),
          Row("TEST_ID_1", "TEST_PARENT_3", 3L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 3L),
          Row("TEST_ID_1", "TEST_PARENT_4", 4L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 4L),
          Row("TEST_ID_1", "TEST_PARENT_5", 5L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 5L),
          Row("TEST_ID_5", "TEST_PARENT_6", 6L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 6L),
          Row("TEST_ID_6", "TEST_PARENT_7", 7L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 7L),
          Row("TEST_ID_7", "TEST_PARENT_8", 8L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 8L),
          Row("TEST_ID_8", "TEST_PARENT_9", 9L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 9L),
          Row("TEST_ID_9", "TEST_PARENT_10", 10L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 10L),
          Row("TEST_ID_10", "TEST_PARENT_11", 11L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 11L)
        )
      }
      val inputDataFrame = sqlContext.createDataFrame(inputData, inputSchema)
      val inputDataIterator = inputDataFrame.rdd.toLocalIterator
      val inputSparkEsBulkWriter = new SparkEsBulkWriter[Row](
        esIndex = "test_index",
        esType = "test_type",
        esClient = () => esServer.client,
        sparkEsSerializer = new SparkEsDataFrameSerializer(inputSchema),
        sparkEsMapper = new SparkEsDataFrameMapper(inputMapperConf),
        sparkEsWriteConf = inputSparkEsWriteConf
      )

      inputSparkEsBulkWriter.write(null, inputDataIterator)

      val outputGetResponse = esServer.client.prepareGet("test_index", "test_type", "TEST_ID_1").get()

      outputGetResponse.isExists mustEqual true
      outputGetResponse.getSource.get("parent").asInstanceOf[String] mustEqual "TEST_PARENT_5"
      outputGetResponse.getSource.get("version").asInstanceOf[Integer] mustEqual 5
      outputGetResponse.getSource.get("routing").asInstanceOf[String] mustEqual "TEST_ROUTING_1"
      outputGetResponse.getSource.get("ttl").asInstanceOf[Integer] mustEqual 86400000
      outputGetResponse.getSource.get("timestamp").asInstanceOf[String] mustEqual "TEST_TIMESTAMP_1"
      outputGetResponse.getSource.get("value").asInstanceOf[Integer] mustEqual 5
    }
  }
}
