package com.github.jparkie.spark.elasticsearch.conf

import java.net.InetSocketAddress

import com.github.jparkie.spark.elasticsearch.util.SparkEsConfParam
import org.apache.spark.SparkConf

import scala.collection.mutable

/**
 * Configurations for EsNativeDataFrameWriter's TransportClient.
 *
 * @param transportAddresses The minimum set of hosts to connect to when establishing a client.
 *                           CONFIG_CLIENT_TRANSPORT_SNIFF is enabled by default.
 * @param transportPort The port to connect when establishing a client.
 * @param transportSettings Miscellaneous settings for the TransportClient.
 *                          Empty by default.
 */
case class SparkEsTransportClientConf(
  transportAddresses: Seq[String],
  transportPort:      Int,
  transportSettings:  Map[String, String]
) extends Serializable

object SparkEsTransportClientConf {
  val CONFIG_CLUSTER_NAME = "cluster.name"
  val CONFIG_CLIENT_TRANSPORT_SNIFF = "client.transport.sniff"
  val CONFIG_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME = "client.transport.ignore_cluster_name"
  val CONFIG_CLIENT_TRANSPORT_PING_TIMEOUT = "client.transport.ping_timeout"
  val CONFIG_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL = "client.transport.nodes_sampler_interval"

  val ES_NODES = SparkEsConfParam[Seq[String]](
    name = "es.nodes",
    default = Seq.empty[String]
  )
  val ES_PORT = SparkEsConfParam[Int](
    name = "es.port",
    default = 9300
  )
  val ES_CLUSTER_NAME = SparkEsConfParam[String](
    name = s"es.$CONFIG_CLUSTER_NAME",
    default = null
  )
  val ES_CLIENT_TRANSPORT_SNIFF = SparkEsConfParam[String](
    name = s"es.$CONFIG_CLIENT_TRANSPORT_SNIFF",
    default = null
  )
  val ES_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME = SparkEsConfParam[String](
    name = s"es.$CONFIG_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME",
    default = null
  )
  val ES_CLIENT_TRANSPORT_PING_TIMEOUT = SparkEsConfParam[String](
    name = s"es.$CONFIG_CLIENT_TRANSPORT_PING_TIMEOUT",
    default = null
  )
  val ES_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL = SparkEsConfParam[String](
    name = s"es.$CONFIG_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL",
    default = null
  )

  def getTransportAddresses(transportAddresses: Seq[String], transportPort: Int): Seq[InetSocketAddress] = {
    transportAddresses match {
      case null | Nil => throw new IllegalArgumentException("A contact point list cannot be empty.")
      case hosts => hosts map {
        ipWithPort =>
          ipWithPort.split(":") match {
            case Array(actualHost, actualPort) =>
              new InetSocketAddress(actualHost, actualPort.toInt)
            case Array(actualHost) =>
              new InetSocketAddress(actualHost, transportPort)
            case errorMessage =>
              throw new IllegalArgumentException(s"A contact point should have the form [host:port] or [host] but was: $errorMessage.")
          }
      }
    }
  }

  /**
   * Extracts SparkEsTransportClientConf from a SparkConf.
   *
   * @param sparkConf A SparkConf.
   * @return A SparkEsTransportClientConf from a SparkConf.
   */
  def fromSparkConf(sparkConf: SparkConf): SparkEsTransportClientConf = {
    val tempEsNodes = ES_NODES.fromConf(sparkConf)((sc, name) => sc.get(name).split(","))
    val tempEsPort = ES_PORT.fromConf(sparkConf)((sc, name) => sc.getInt(name, ES_PORT.default))
    val tempSettings = mutable.HashMap.empty[String, String]

    require(
      tempEsNodes.nonEmpty,
      s"""No nodes defined in property ${ES_NODES.name} is in SparkConf.""".stripMargin
    )

    if (sparkConf.contains(ES_CLUSTER_NAME.name) || sparkConf.contains(s"spark.${ES_CLUSTER_NAME.name}"))
      tempSettings.put(CONFIG_CLUSTER_NAME, ES_CLUSTER_NAME.fromConf(sparkConf)((sc, name) => sc.get(name)))

    if (sparkConf.contains(ES_CLIENT_TRANSPORT_SNIFF.name) || sparkConf.contains(s"spark.${ES_CLIENT_TRANSPORT_SNIFF.name}"))
      tempSettings.put(CONFIG_CLIENT_TRANSPORT_SNIFF, ES_CLIENT_TRANSPORT_SNIFF.fromConf(sparkConf)((sc, name) => sc.get(name)))

    if (sparkConf.contains(ES_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME.name) || sparkConf.contains(s"spark.${ES_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME.name}"))
      tempSettings.put(CONFIG_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME, ES_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME.fromConf(sparkConf)((sc, name) => sc.get(name)))

    if (sparkConf.contains(ES_CLIENT_TRANSPORT_PING_TIMEOUT.name) || sparkConf.contains(s"spark.${ES_CLIENT_TRANSPORT_PING_TIMEOUT.name}"))
      tempSettings.put(CONFIG_CLIENT_TRANSPORT_PING_TIMEOUT, ES_CLIENT_TRANSPORT_PING_TIMEOUT.fromConf(sparkConf)((sc, name) => sc.get(name)))

    if (sparkConf.contains(ES_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL.name) || sparkConf.contains(s"spark.${ES_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL.name}"))
      tempSettings.put(CONFIG_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL, ES_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL.fromConf(sparkConf)((sc, name) => sc.get(name)))

    SparkEsTransportClientConf(
      transportAddresses = tempEsNodes,
      transportPort = tempEsPort,
      transportSettings = tempSettings.toMap
    )
  }
}