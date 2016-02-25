package com.github.jparkie.spark.elasticsearch.conf

import org.apache.spark.SparkConf
import org.scalatest.{MustMatchers, WordSpec}

class SparkEsWriteConfSpec extends WordSpec with MustMatchers {
  "SparkEsWriteConf" must {
    "be extracted from SparkConf successfully" in {
      val inputSparkConf = new SparkConf()
        .set("es.batch.size.entries", "1")
        .set("es.batch.size.bytes", "2")
        .set("es.batch.concurrent.request", "3")
        .set("es.batch.flush.timeout", "4")

      val expectedSparkEsWriteConf = SparkEsWriteConf(
        bulkActions = 1,
        bulkSizeInMB = 2,
        concurrentRequests = 3,
        flushTimeoutInSeconds = 4
      )

      val outputSparkEsWriteConf = SparkEsWriteConf.fromSparkConf(inputSparkConf)

      outputSparkEsWriteConf mustEqual expectedSparkEsWriteConf
    }
  }
}
