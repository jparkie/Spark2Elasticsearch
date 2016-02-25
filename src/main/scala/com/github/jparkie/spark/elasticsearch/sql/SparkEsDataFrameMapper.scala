package com.github.jparkie.spark.elasticsearch.sql

import com.github.jparkie.spark.elasticsearch.SparkEsMapper
import com.github.jparkie.spark.elasticsearch.conf.SparkEsMapperConf
import org.apache.spark.sql.Row
import org.elasticsearch.index.VersionType

/**
 * Extracts mappings from a Row for an IndexRequest.
 *
 * @param mapperConf Configurations for IndexRequest.
 */
class SparkEsDataFrameMapper(mapperConf: SparkEsMapperConf) extends SparkEsMapper[Row] {
  import SparkEsDataFrameMapper._
  import SparkEsMapperConf._

  /**
   * Extracts the document field/property name containing the document id.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document id.
   */
  override def extractMappingId(value: Row): Option[String] = {
    mapperConf.esMappingId
      .map(currentFieldName => value.getAsToString(currentFieldName))
  }

  /**
   * Extracts the document field/property name containing the document parent.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document parent.
   */
  override def extractMappingParent(value: Row): Option[String] = {
    mapperConf.esMappingParent.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue
      case currentFieldName =>
        value.getAsToString(currentFieldName)
    }
  }

  /**
   * Extracts the document field/property name containing the document version.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document version.
   */
  override def extractMappingVersion(value: Row): Option[Long] = {
    mapperConf.esMappingVersion.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue.toLong
      case currentFieldName =>
        value.getAsToString(currentFieldName).toLong
    }
  }

  /**
   * Extracts the type of versioning used.
   *
   * @param value Object to extract mappings.
   * @return The type of versioning used..
   */
  override def extractMappingVersionType(value: Row): Option[VersionType] = {
    mapperConf.esMappingVersion
      .flatMap(_ => mapperConf.esMappingVersionType.map(VersionType.fromString))
  }

  /**
   * Extracts the document field/property name containing the document routing.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document routing.
   */
  override def extractMappingRouting(value: Row): Option[String] = {
    mapperConf.esMappingRouting.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue
      case currentFieldName =>
        value.getAsToString(currentFieldName)
    }
  }

  /**
   * Extracts the document field/property name containing the document time-to-live.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document time-to-live.
   */
  override def extractMappingTTLInMillis(value: Row): Option[Long] = {
    mapperConf.esMappingTTLInMillis.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue.toLong
      case currentFieldName =>
        value.getAsToString(currentFieldName).toLong
    }
  }

  /**
   * Extracts the document field/property name containing the document timestamp.
   *
   * @param value Object to extract mappings.
   * @return The document field/property name containing the document timestamp.
   */
  override def extractMappingTimestamp(value: Row): Option[String] = {
    mapperConf.esMappingTimestamp.map {
      case CONSTANT_FIELD_REGEX(constantValue) =>
        constantValue
      case currentFieldName =>
        value.getAsToString(currentFieldName)
    }
  }
}

object SparkEsDataFrameMapper {
  /**
   * Adds method to retrieve field as String through Any's toString.
   *
   * @param currentRow Row object.
   */
  implicit class RichRow(currentRow: Row) {
    // TODO: Find a better and safer way.
    def getAsToString(fieldName: String): String = {
      currentRow.getAs[Any](fieldName).toString
    }
  }
}