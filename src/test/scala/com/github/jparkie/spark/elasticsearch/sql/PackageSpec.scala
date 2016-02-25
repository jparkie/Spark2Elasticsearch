package com.github.jparkie.spark.elasticsearch.sql

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{MustMatchers, WordSpec}

class PackageSpec extends WordSpec with MustMatchers with SharedSparkContext {
  "Package com.github.jparkie.spark.elasticsearch.sql" must {
    "lift DataFrame into SparkEsDataFrameFunctions" in {

      val sqlContext = new SQLContext(sc)

      val inputData = Seq(
        ("TEST_VALUE_1", 1),
        ("TEST_VALUE_2", 2),
        ("TEST_VALUE_3", 3)
      )

      val outputDataFrame = sqlContext.createDataFrame(inputData)
        .toDF("key", "value")

      // If sparkContext is available, DataFrame was lifted into SparkEsDataFrameFunctions.
      outputDataFrame.sparkContext
    }
  }
}
