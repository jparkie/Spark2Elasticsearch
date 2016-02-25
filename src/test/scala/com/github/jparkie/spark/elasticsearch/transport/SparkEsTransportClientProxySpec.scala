package com.github.jparkie.spark.elasticsearch.transport

import com.github.jparkie.spark.elasticsearch.util.SparkEsException
import org.scalatest.{ MustMatchers, WordSpec }

class SparkEsTransportClientProxySpec extends WordSpec with MustMatchers {
  "SparkEsTransportClientProxy" must {
    "prohibit close() call" in {
      val inputSparkEsTransportClientProxy = new SparkEsTransportClientProxy(null)

      val outputException = intercept[SparkEsException] {
        inputSparkEsTransportClientProxy.close()
      }

      outputException.getMessage must include("close() is not supported in SparkEsTransportClientProxy. Please close with SparkEsTransportClientManager.")
    }
  }
}
