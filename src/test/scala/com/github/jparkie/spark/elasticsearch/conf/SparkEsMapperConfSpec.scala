package com.github.jparkie.spark.elasticsearch.conf

import org.apache.spark.SparkConf
import org.scalatest.{ MustMatchers, WordSpec }

class SparkEsMapperConfSpec extends WordSpec with MustMatchers {
  "SparkEsMapperConf" must {
    "be extracted from SparkConf successfully" in {
      val inputSparkConf = new SparkConf()
        .set("es.mapping.id", "TEST_VALUE_1")
        .set("es.mapping.parent", "TEST_VALUE_2")
        .set("es.mapping.version", "TEST_VALUE_3")
        .set("es.mapping.version.type", "TEST_VALUE_4")
        .set("es.mapping.routing", "TEST_VALUE_5")
        .set("es.mapping.ttl", "TEST_VALUE_6")
        .set("es.mapping.timestamp", "TEST_VALUE_7")

      val expectedSparkEsMapperConf = SparkEsMapperConf(
        esMappingId = Some("TEST_VALUE_1"),
        esMappingParent = Some("TEST_VALUE_2"),
        esMappingVersion = Some("TEST_VALUE_3"),
        esMappingVersionType = Some("TEST_VALUE_4"),
        esMappingRouting = Some("TEST_VALUE_5"),
        esMappingTTLInMillis = Some("TEST_VALUE_6"),
        esMappingTimestamp = Some("TEST_VALUE_7")
      )

      val outputSparkEsMapperConf = SparkEsMapperConf.fromSparkConf(inputSparkConf)

      outputSparkEsMapperConf mustEqual expectedSparkEsMapperConf
    }

    "extract CONSTANT_FIELD_REGEX successfully" in {
      val inputString = "<TEST_VALUE_1>"

      val expectedString = "TEST_VALUE_1"

      val outputString = inputString match {
        case SparkEsMapperConf.CONSTANT_FIELD_REGEX(outputString) =>
          outputString
        case _ =>
          fail("CONSTANT_FIELD_REGEX failed.")
      }

      outputString mustEqual expectedString
    }
  }
}
