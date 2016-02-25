package com.github.jparkie.spark.elasticsearch

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.{Node, NodeBuilder}

/**
  * Local Elasticsearch server of one node for integration testing.
  */
class ElasticSearchServer {
  private val homeDir = Files.createTempDirectory("elasticsearch").toFile
  private val dataDir = Files.createTempDirectory("elasticsearch").toFile

  val clusterName = "Spark2Elasticsearch"

  private lazy val internalNode: Node = {
    val tempSettings = Settings.builder()
      .put("path.home", homeDir.getAbsolutePath)
      .put("path.data", dataDir.getAbsolutePath)
      .put("es.logger.level", "OFF")
      .build()
    val tempNode = NodeBuilder.nodeBuilder()
      .clusterName(clusterName)
      .local(true)
      .data(true)
      .settings(tempSettings)
      .build()

    tempNode
  }

  /**
    * Fetch a client to the Elasticsearch cluster.
    *
    * @return A Client.
    */
  def client: Client = {
    internalNode.client()
  }

  /**
    * Start the Elasticsearch cluster.
    */
  def start(): Unit = {
    internalNode.start()
  }

  /**
    * Stop the Elasticsearch cluster.
    */
  def stop(): Unit = {
    internalNode.close()

    try {
      FileUtils.forceDelete(homeDir)
      FileUtils.forceDelete(dataDir)
    } catch {
      case e: Exception =>
        // Do Nothing.
    }
  }

  /**
    * Create Index.
    *
    * @param index The name of Index.
    */
  def createAndWaitForIndex(index: String): Unit = {
    client.admin.indices.prepareCreate(index).execute.actionGet()
    client.admin.cluster.prepareHealth(index).setWaitForActiveShards(1).execute.actionGet()
  }
}
