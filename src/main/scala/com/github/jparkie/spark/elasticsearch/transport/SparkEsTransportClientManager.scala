package com.github.jparkie.spark.elasticsearch.transport

import com.github.jparkie.spark.elasticsearch.conf.SparkEsTransportClientConf
import org.apache.spark.Logging
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.collection.mutable

private[elasticsearch] trait SparkEsTransportClientManager extends Serializable with Logging {
  @transient
  private[transport] val internalTransportClients = mutable.HashMap.empty[SparkEsTransportClientConf, TransportClient]

  private[transport] def buildTransportSettings(clientConf: SparkEsTransportClientConf): Settings = {
    val esSettingsBuilder = Settings.builder()

    clientConf.transportSettings foreach { currentSetting =>
      esSettingsBuilder.put(currentSetting._1, currentSetting._2)
    }

    esSettingsBuilder.build()
  }

  private[transport] def buildTransportClient(clientConf: SparkEsTransportClientConf, esSettings: Settings): TransportClient = {
    import SparkEsTransportClientConf._

    val esClient = TransportClient.builder()
      .settings(esSettings)
      .build()

    getTransportAddresses(clientConf.transportAddresses, clientConf.transportPort) foreach { inetSocketAddress =>
      esClient.addTransportAddresses(new InetSocketTransportAddress(inetSocketAddress))
    }

    sys.addShutdownHook {
      logInfo("Closed Elasticsearch Transport Client.")

      esClient.close()
    }

    logInfo(s"Connected to the following Elasticsearch nodes: ${esClient.connectedNodes()}.")

    esClient
  }

  /**
   * Gets or creates a TransportClient per JVM.
   *
   * @param clientConf Settings and initial endpoints for connection.
   * @return SparkEsTransportClientProxy as Client.
   */
  def getTransportClient(clientConf: SparkEsTransportClientConf): Client = synchronized {
    internalTransportClients.get(clientConf) match {
      case Some(transportClient) =>
        new SparkEsTransportClientProxy(transportClient)
      case None =>
        val transportSettings = buildTransportSettings(clientConf)
        val transportClient = buildTransportClient(clientConf, transportSettings)
        internalTransportClients.put(clientConf, transportClient)
        new SparkEsTransportClientProxy(transportClient)
    }
  }

  /**
   * Evicts and closes a TransportClient.
   *
   * @param clientConf Settings and initial endpoints for connection.
   */
  def closeTransportClient(clientConf: SparkEsTransportClientConf): Unit = synchronized {
    internalTransportClients.remove(clientConf) match {
      case Some(transportClient) =>
        transportClient.close()
      case None =>
        logError(s"No TransportClient for $clientConf.")
    }
  }
}

object SparkEsTransportClientManager extends SparkEsTransportClientManager