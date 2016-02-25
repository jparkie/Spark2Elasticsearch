package com.github.jparkie.spark.elasticsearch.transport

import com.github.jparkie.spark.elasticsearch.conf.SparkEsTransportClientConf
import org.scalatest.{ MustMatchers, WordSpec }

class SparkEsTransportClientManagerSpec extends WordSpec with MustMatchers {
  "SparkEsTransportClientManager" must {
    "maintain one unique TransportClient in internalTransportClients" in {
      val inputClientConf = SparkEsTransportClientConf(
        transportAddresses = Seq("127.0.0.1"),
        transportPort = 9300,
        transportSettings = Map.empty[String, String]
      )
      val inputSparkEsTransportClientManager = new SparkEsTransportClientManager {}

      inputSparkEsTransportClientManager.getTransportClient(inputClientConf)
      inputSparkEsTransportClientManager.getTransportClient(inputClientConf)

      inputSparkEsTransportClientManager.internalTransportClients.size mustEqual 1

      inputSparkEsTransportClientManager.closeTransportClient(inputClientConf)
    }

    "return a SparkEsTransportClientProxy when calling getTransportClient()" in {
      val inputClientConf = SparkEsTransportClientConf(
        transportAddresses = Seq("127.0.0.1"),
        transportPort = 9300,
        transportSettings = Map.empty[String, String]
      )
      val inputSparkEsTransportClientManager = new SparkEsTransportClientManager {}

      val outputClient = inputSparkEsTransportClientManager.getTransportClient(inputClientConf)

      outputClient.getClass mustEqual classOf[SparkEsTransportClientProxy]

      inputSparkEsTransportClientManager.closeTransportClient(inputClientConf)
    }

    "evict TransportClient after calling closeTransportClient" in {
      val inputClientConf = SparkEsTransportClientConf(
        transportAddresses = Seq("127.0.0.1"),
        transportPort = 9300,
        transportSettings = Map.empty[String, String]
      )
      val inputSparkEsTransportClientManager = new SparkEsTransportClientManager {}

      inputSparkEsTransportClientManager.getTransportClient(inputClientConf)

      inputSparkEsTransportClientManager.closeTransportClient(inputClientConf)

      inputSparkEsTransportClientManager.internalTransportClients.size mustEqual 0
    }

    "returns buildTransportSettings() successfully" in {
      val inputClientConf = SparkEsTransportClientConf(
        transportAddresses = Seq("127.0.0.1"),
        transportPort = 9300,
        transportSettings = Map(
          "TEST_KEY_1" -> "TEST_VALUE_1",
          "TEST_KEY_2" -> "TEST_VALUE_2",
          "TEST_KEY_3" -> "TEST_VALUE_3"
        )
      )
      val inputSparkEsTransportClientManager = new SparkEsTransportClientManager {}

      val outputSettings = inputSparkEsTransportClientManager.buildTransportSettings(inputClientConf)

      outputSettings.get("TEST_KEY_1") mustEqual "TEST_VALUE_1"
      outputSettings.get("TEST_KEY_2") mustEqual "TEST_VALUE_2"
      outputSettings.get("TEST_KEY_3") mustEqual "TEST_VALUE_3"
    }

    "returns buildTransportClient() successfully" in {
      val inputClientConf = SparkEsTransportClientConf(
        transportAddresses = Seq("127.0.0.1"),
        transportPort = 9300,
        transportSettings = Map.empty[String, String]
      )
      val inputSparkEsTransportClientManager = new SparkEsTransportClientManager {}

      val outputSettings = inputSparkEsTransportClientManager.buildTransportSettings(inputClientConf)
      val outputClient = inputSparkEsTransportClientManager.buildTransportClient(inputClientConf, outputSettings)
      val outputHost = outputClient.transportAddresses().get(0).getHost
      val outputPort = outputClient.transportAddresses().get(0).getPort

      outputHost mustEqual "127.0.0.1"
      outputPort mustEqual 9300

      outputClient.close()
    }
  }
}
