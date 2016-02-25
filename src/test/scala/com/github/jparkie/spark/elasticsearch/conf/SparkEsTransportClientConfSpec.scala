package com.github.jparkie.spark.elasticsearch.conf

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.scalatest.{MustMatchers, WordSpec}

class SparkEsTransportClientConfSpec extends WordSpec with MustMatchers {
  "SparkEsTransportClientConf" must {
    "be extracted from SparkConf successfully" in {
      val inputSparkConf = new SparkConf()
        .set("es.nodes", "127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002")
        .set("es.port", "1337")

      val expectedSparkEsTransportClientConf = SparkEsTransportClientConf(
        transportAddresses = Seq("127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"),
        transportPort = 1337,
        transportSettings = Map.empty[String, String]
      )

      val outputSparkEsTransportClientConf = SparkEsTransportClientConf.fromSparkConf(inputSparkConf)

      outputSparkEsTransportClientConf mustEqual expectedSparkEsTransportClientConf
    }

    "be extracted from SparkConf unsuccessfully" in {
      val inputSparkConf = new SparkConf()

      val outputException = intercept[IllegalArgumentException] {
        SparkEsTransportClientConf.fromSparkConf(inputSparkConf)
      }

      outputException.getMessage must include("Property es.nodes is not provided in SparkConf.")
    }

    "extract transportSettings successfully" in {
      val inputSparkConf = new SparkConf()
        .set("es.nodes", "127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002")
        .set("es.port", "1337")
        .set("es.cluster.name", "TEST_VALUE_1")
        .set("es.client.transport.sniff", "TEST_VALUE_2")
        .set("es.client.transport.ignore_cluster_name", "TEST_VALUE_3")
        .set("es.client.transport.ping_timeout", "TEST_VALUE_4")
        .set("es.client.transport.nodes_sampler_interval", "TEST_VALUE_5")

      val expectedSparkEsTransportClientConf = SparkEsTransportClientConf(
        transportAddresses = Seq("127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"),
        transportPort = 1337,
        transportSettings = Map(
          "cluster.name" -> "TEST_VALUE_1",
          "client.transport.sniff" -> "TEST_VALUE_2",
          "client.transport.ignore_cluster_name" -> "TEST_VALUE_3",
          "client.transport.ping_timeout" -> "TEST_VALUE_4",
          "client.transport.nodes_sampler_interval" -> "TEST_VALUE_5"
        )
      )

      val outputSparkEsTransportClientConf = SparkEsTransportClientConf.fromSparkConf(inputSparkConf)

      outputSparkEsTransportClientConf mustEqual expectedSparkEsTransportClientConf
    }

    "extract transportAddresses as Seq[InetSocketAddress] successfully with port secondly" in {
      val inputAddresses = Seq("127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002")
      val inputPort = 1337

      val expectedTransportAddresses = Seq(
        new InetSocketAddress("127.0.0.1", 9000),
        new InetSocketAddress("127.0.0.1", 9001),
        new InetSocketAddress("127.0.0.1", 9002)
      )

      val outputTransportAddresses = SparkEsTransportClientConf.getTransportAddresses(inputAddresses, inputPort)

      outputTransportAddresses mustEqual expectedTransportAddresses
    }
  }
}
