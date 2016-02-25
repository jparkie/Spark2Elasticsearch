package com.github.jparkie.spark.elasticsearch

import org.elasticsearch.index.VersionType

/**
 * Extracts mappings from a T for an IndexRequest.
 *
 * @tparam T Object to extract mappings.
 */
trait SparkEsMapper[T] extends Serializable {
  /**
   * Extracts the document field/property name containing the document id.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document id.
   */
  def extractMappingId(value: T): Option[String]

  /**
   * Extracts the document field/property name containing the document parent.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document parent.
   */
  def extractMappingParent(value: T): Option[String]

  /**
   * Extracts the document field/property name containing the document version.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document version.
   */
  def extractMappingVersion(value: T): Option[Long]

  /**
   * Extracts the type of versioning used.
   *
   * @param value Object to extract mappings.
   * @return The type of versioning used..
   */
  def extractMappingVersionType(value: T): Option[VersionType]

  /**
   * Extracts the document field/property name containing the document routing.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document routing.
   */
  def extractMappingRouting(value: T): Option[String]

  /**
   * Extracts the document field/property name containing the document time-to-live.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document time-to-live.
   */
  def extractMappingTTLInMillis(value: T): Option[Long]

  /**
   * Extracts the document field/property name containing the document timestamp.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document timestamp.
   */
  def extractMappingTimestamp(value: T): Option[String]
}
