package com.github.jparkie.spark.elasticsearch.transport

import com.github.jparkie.spark.elasticsearch.conf.SparkEsTransportClientConf
import com.github.jparkie.spark.elasticsearch.util.SparkEsException
import org.scalatest.{ MustMatchers, WordSpec }

class SparkEsTransportClientProxySpec extends WordSpec with MustMatchers {
  "SparkEsTransportClientProxy" must {
    "prohibit close() call" in {
      val inputClientConf = SparkEsTransportClientConf(
        transportAddresses = Seq("127.0.0.1"),
        transportPort = 9300,
        transportSettings = Map.empty[String, String]
      )
      val inputSparkEsTransportClientManager = new SparkEsTransportClientManager {}
      val inputSparkEsTransportClient = inputSparkEsTransportClientManager.getTransportClient(inputClientConf)
      val inputSparkEsTransportClientProxy = new SparkEsTransportClientProxy(inputSparkEsTransportClient)

      val outputException = intercept[SparkEsException] {
        inputSparkEsTransportClientProxy.close()
      }

      outputException.getMessage must include("close() is not supported in SparkEsTransportClientProxy. Please close with SparkEsTransportClientManager.")
    }
  }
}
