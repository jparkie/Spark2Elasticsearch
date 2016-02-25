package com.github.jparkie.spark.elasticsearch.sql

import java.sql.{ Date, Timestamp }

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Row, SQLContext }
import org.elasticsearch.common.xcontent.XContentFactory
import org.scalatest.{ MustMatchers, WordSpec }

class SparkEsDataFrameSerializerSpec extends WordSpec with MustMatchers with SharedSparkContext {
  "SparkEsDataFrameSerializer" must {
    "execute writeStruct() successfully" in {
      val sqlContext = new SQLContext(sc)

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
      val inputSparkEsDataFrameSerializer = new SparkEsDataFrameSerializer(inputSchema)

      val inputBuilder = XContentFactory.jsonBuilder()
      val outputBuilder = inputSparkEsDataFrameSerializer.writeStruct(inputSchema, inputRow, inputBuilder)
      outputBuilder.string() must include("""{"id":"TEST_ID_1","parent":"TEST_PARENT_1","version":1,"routing":"TEST_ROUTING_1","ttl":86400000,"timestamp":"TEST_TIMESTAMP_1","value":1}""")
      inputBuilder.close()
    }

    "execute writeArray() successfully" in {
      val inputSparkEsDataFrameSerializer = new SparkEsDataFrameSerializer(null)

      val inputArray = Array(1, 2, 3)
      val inputBuilder = XContentFactory.jsonBuilder()
      val outputBuilder = inputSparkEsDataFrameSerializer.writeArray(ArrayType(IntegerType), inputArray, inputBuilder)
      outputBuilder.string() must include("""1,2,3""")
      inputBuilder.close()
    }

    "execute writeMap() successfully" in {
      val inputSparkEsDataFrameSerializer = new SparkEsDataFrameSerializer(null)

      val inputMap = Map(
        "TEST_KEY_1" -> "TEST_VALUE_1",
        "TEST_KEY_2" -> "TEST_VALUE_3",
        "TEST_KEY_3" -> "TEST_VALUE_3"
      )
      val inputBuilder = XContentFactory.jsonBuilder()
      val outputBuilder = inputSparkEsDataFrameSerializer.writeMap(MapType(StringType, StringType), inputMap, inputBuilder)
      outputBuilder.string() must include("""{"TEST_KEY_1":"TEST_VALUE_1","TEST_KEY_2":"TEST_VALUE_3","TEST_KEY_3":"TEST_VALUE_3"}""")
      inputBuilder.close()
    }

    "execute writePrimitive() successfully" in {
      val inputSparkEsDataFrameSerializer = new SparkEsDataFrameSerializer(null)

      val inputBuilder1 = XContentFactory.jsonBuilder()
      val outputBuilder1 = inputSparkEsDataFrameSerializer.writePrimitive(BinaryType, Array[Byte](1), inputBuilder1)
      outputBuilder1.string() must include("AQ==")
      inputBuilder1.close()

      val inputBuilder2 = XContentFactory.jsonBuilder()
      val outputBuilder2 = inputSparkEsDataFrameSerializer.writePrimitive(BooleanType, true, inputBuilder2)
      outputBuilder2.string() mustEqual "true"
      inputBuilder2.close()

      val inputBuilder3 = XContentFactory.jsonBuilder()
      val outputBuilder3 = inputSparkEsDataFrameSerializer.writePrimitive(ByteType, 1.toByte, inputBuilder3)
      outputBuilder3.string() mustEqual "1"
      inputBuilder3.close()

      val inputBuilder4 = XContentFactory.jsonBuilder()
      val outputBuilder4 = inputSparkEsDataFrameSerializer.writePrimitive(ShortType, 1.toShort, inputBuilder4)
      outputBuilder4.string() mustEqual "1"
      inputBuilder4.close()

      val inputBuilder5 = XContentFactory.jsonBuilder()
      val outputBuilder5 = inputSparkEsDataFrameSerializer.writePrimitive(IntegerType, 1.toInt, inputBuilder5)
      outputBuilder5.string() mustEqual "1"
      inputBuilder5.close()

      val inputBuilder6 = XContentFactory.jsonBuilder()
      val outputBuilder6 = inputSparkEsDataFrameSerializer.writePrimitive(LongType, 1.toLong, inputBuilder6)
      outputBuilder6.string() mustEqual "1"
      inputBuilder6.close()

      val inputBuilder7 = XContentFactory.jsonBuilder()
      val outputBuilder7 = inputSparkEsDataFrameSerializer.writePrimitive(DoubleType, 1.0, inputBuilder7)
      outputBuilder7.string() mustEqual "1.0"
      inputBuilder7.close()

      val inputBuilder8 = XContentFactory.jsonBuilder()
      val outputBuilder8 = inputSparkEsDataFrameSerializer.writePrimitive(FloatType, 1.0F, inputBuilder8)
      outputBuilder8.string() mustEqual "1.0"
      inputBuilder8.close()

      val inputBuilder9 = XContentFactory.jsonBuilder()
      val outputBuilder9 = inputSparkEsDataFrameSerializer.writePrimitive(TimestampType, new Timestamp(834120000000L), inputBuilder9)
      outputBuilder9.string() must include("1996-06-07")
      inputBuilder9.close()

      val inputBuilder10 = XContentFactory.jsonBuilder()
      val outputBuilder10 = inputSparkEsDataFrameSerializer.writePrimitive(DateType, new Date(834120000000L), inputBuilder10)
      outputBuilder10.string() must include("1996-06-07")
      inputBuilder10.close()

      val inputBuilder11 = XContentFactory.jsonBuilder()
      val outputBuilder11 = inputSparkEsDataFrameSerializer.writePrimitive(StringType, "TEST_VALUE", inputBuilder11)
      outputBuilder11.string() must include("TEST_VALUE")
      inputBuilder11.close()
    }
  }
}
