package com.github.jparkie.spark.elasticsearch.transport

import com.github.jparkie.spark.elasticsearch.util.SparkEsException
import org.elasticsearch.client.{ FilterClient, Client }

/**
 * Restrict access to TransportClient by disabling close() without use of SparkEsTransportClientManager.
 */
class SparkEsTransportClientProxy(client: Client) extends FilterClient(client) {
  override def close(): Unit = {
    throw new SparkEsException("close() is not supported in SparkEsTransportClientProxy. Please close with SparkEsTransportClientManager.")
  }
}
