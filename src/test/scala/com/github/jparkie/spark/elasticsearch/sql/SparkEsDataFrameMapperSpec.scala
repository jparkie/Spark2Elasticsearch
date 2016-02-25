package com.github.jparkie.spark.elasticsearch.sql

import com.github.jparkie.spark.elasticsearch.conf.SparkEsMapperConf
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.elasticsearch.index.VersionType
import org.scalatest.{MustMatchers, WordSpec}

class SparkEsDataFrameMapperSpec extends WordSpec with MustMatchers with SharedSparkContext {
  def createInputRow(sparkContext: SparkContext): Row = {
    val sqlContext = new SQLContext(sparkContext)

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
        Row("TEST_ID_1", "TEST_PARENT_1", 1L, "TEST_ROUTING_1", 86400000L, "TEST_TIMESTAMP_1", 1L)
      )
    }
    val inputDataFrame = sqlContext.createDataFrame(inputData, inputSchema)
    val inputRow = inputDataFrame.first()

    inputRow
  }

  "SparkEsDataFrameMapper" must {
    "from Row extract noting successfully" in {
      val inputRow = createInputRow(sc)
      val inputMapperConf = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )

      val outputSparkEsDataFrameMapper = new SparkEsDataFrameMapper(inputMapperConf)

      outputSparkEsDataFrameMapper.extractMappingId(inputRow) mustEqual None
      outputSparkEsDataFrameMapper.extractMappingParent(inputRow) mustEqual None
      outputSparkEsDataFrameMapper.extractMappingVersion(inputRow) mustEqual None
      outputSparkEsDataFrameMapper.extractMappingVersionType(inputRow) mustEqual None
      outputSparkEsDataFrameMapper.extractMappingRouting(inputRow) mustEqual None
      outputSparkEsDataFrameMapper.extractMappingTTLInMillis(inputRow) mustEqual None
      outputSparkEsDataFrameMapper.extractMappingTimestamp(inputRow) mustEqual None
    }

    "from Row execute extractMappingId() successfully" in {
      val inputRow = createInputRow(sc)
      val inputMapperConf = SparkEsMapperConf(
        esMappingId = Some("id"),
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )

      val outputSparkEsDataFrameMapper = new SparkEsDataFrameMapper(inputMapperConf)

      outputSparkEsDataFrameMapper.extractMappingId(inputRow) mustEqual Some("TEST_ID_1")
    }

    "from Row execute extractMappingParent() successfully" in {
      val inputRow = createInputRow(sc)
      val inputMapperConf1 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = Some("parent"),
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )
      val inputMapperConf2 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = Some("<TEST_VALUE>"),
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )

      val outputSparkEsDataFrameMapper1 = new SparkEsDataFrameMapper(inputMapperConf1)
      val outputSparkEsDataFrameMapper2 = new SparkEsDataFrameMapper(inputMapperConf2)

      outputSparkEsDataFrameMapper1.extractMappingParent(inputRow) mustEqual Some("TEST_PARENT_1")
      outputSparkEsDataFrameMapper2.extractMappingParent(inputRow) mustEqual Some("TEST_VALUE")
    }

    "from Row execute extractMappingVersion() successfully" in {
      val inputRow = createInputRow(sc)
      val inputMapperConf1 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = Some("version"),
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )
      val inputMapperConf2 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = Some("<1337>"),
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )

      val outputSparkEsDataFrameMapper1 = new SparkEsDataFrameMapper(inputMapperConf1)
      val outputSparkEsDataFrameMapper2 = new SparkEsDataFrameMapper(inputMapperConf2)

      outputSparkEsDataFrameMapper1.extractMappingVersion(inputRow) mustEqual Some(1L)
      outputSparkEsDataFrameMapper2.extractMappingVersion(inputRow) mustEqual Some(1337L)
    }

    "from Row execute extractMappingVersionType() successfully" in {
      val inputRow = createInputRow(sc)
      val inputMapperConf1 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = Some("<TEST_VALUE>"),
        esMappingVersionType = Some("force"),
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )
      val inputMapperConf2 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = Some("force"),
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )

      val outputSparkEsDataFrameMapper1 = new SparkEsDataFrameMapper(inputMapperConf1)
      val outputSparkEsDataFrameMapper2 = new SparkEsDataFrameMapper(inputMapperConf2)

      outputSparkEsDataFrameMapper1.extractMappingVersionType(inputRow) mustEqual Some(VersionType.FORCE)
      outputSparkEsDataFrameMapper2.extractMappingVersionType(inputRow) mustEqual None
    }

    "from Row execute extractMappingRouting() successfully" in {
      val inputRow = createInputRow(sc)
      val inputMapperConf1 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = Some("routing"),
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )
      val inputMapperConf2 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = Some("<TEST_VALUE>"),
        esMappingTTLInMillis = None,
        esMappingTimestamp = None
      )

      val outputSparkEsDataFrameMapper1 = new SparkEsDataFrameMapper(inputMapperConf1)
      val outputSparkEsDataFrameMapper2 = new SparkEsDataFrameMapper(inputMapperConf2)

      outputSparkEsDataFrameMapper1.extractMappingRouting(inputRow) mustEqual Some("TEST_ROUTING_1")
      outputSparkEsDataFrameMapper2.extractMappingRouting(inputRow) mustEqual Some("TEST_VALUE")
    }

    "from Row execute extractMappingTTLInMillis() successfully" in {
      val inputRow = createInputRow(sc)
      val inputMapperConf1 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = Some("ttl"),
        esMappingTimestamp = None
      )
      val inputMapperConf2 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = Some("<1337>"),
        esMappingTimestamp = None
      )

      val outputSparkEsDataFrameMapper1 = new SparkEsDataFrameMapper(inputMapperConf1)
      val outputSparkEsDataFrameMapper2 = new SparkEsDataFrameMapper(inputMapperConf2)

      outputSparkEsDataFrameMapper1.extractMappingTTLInMillis(inputRow) mustEqual Some(86400000L)
      outputSparkEsDataFrameMapper2.extractMappingTTLInMillis(inputRow) mustEqual Some(1337L)
    }

    "from Row execute extractMappingTimestamp() successfully" in {
      val inputRow = createInputRow(sc)
      val inputMapperConf1 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = Some("timestamp")
      )
      val inputMapperConf2 = SparkEsMapperConf(
        esMappingId = None,
        esMappingParent = None,
        esMappingVersion = None,
        esMappingVersionType = None,
        esMappingRouting = None,
        esMappingTTLInMillis = None,
        esMappingTimestamp = Some("<TEST_VALUE>")
      )

      val outputSparkEsDataFrameMapper1 = new SparkEsDataFrameMapper(inputMapperConf1)
      val outputSparkEsDataFrameMapper2 = new SparkEsDataFrameMapper(inputMapperConf2)

      outputSparkEsDataFrameMapper1.extractMappingTimestamp(inputRow) mustEqual Some("TEST_TIMESTAMP_1")
      outputSparkEsDataFrameMapper2.extractMappingTimestamp(inputRow) mustEqual Some("TEST_VALUE")
    }
  }
}
