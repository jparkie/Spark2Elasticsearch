package com.github.jparkie.spark.elasticsearch.conf

import com.github.jparkie.spark.elasticsearch.util.SparkEsConfParam
import org.apache.spark.SparkConf

/**
 * https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping
 *
 * @param esMappingId The document field/property name containing the document id.
 * @param esMappingParent The document field/property name containing the document parent.
 *                        To specify a constant, use the <CONSTANT> format.
 * @param esMappingVersion The document field/property name containing the document version.
 *                         To specify a constant, use the <CONSTANT> format.
 * @param esMappingVersionType Indicates the type of versioning used.
 *                             http://www.elastic.co/guide/en/elasticsearch/reference/2.0/docs-index_.html#_version_types
 *                             If es.mapping.version is undefined (default), its value is unspecified.
 *                             If es.mapping.version is specified, its value becomes external.
 * @param esMappingRouting The document field/property name containing the document routing.
 *                         To specify a constant, use the <CONSTANT> format.
 * @param esMappingTTLInMillis The document field/property name containing the document time-to-live.
 *                     To specify a constant, use the <CONSTANT> format.
 * @param esMappingTimestamp The document field/property name containing the document timestamp.
 *                           To specify a constant, use the <CONSTANT> format.
 */

case class SparkEsMapperConf(
  esMappingId:          Option[String],
  esMappingParent:      Option[String],
  esMappingVersion:     Option[String],
  esMappingVersionType: Option[String],
  esMappingRouting:     Option[String],
  esMappingTTLInMillis: Option[String],
  esMappingTimestamp:   Option[String]
) extends Serializable

object SparkEsMapperConf {
  val CONSTANT_FIELD_REGEX = """\<([^>]+)\>""".r

  val ES_MAPPING_ID = SparkEsConfParam[Option[String]](
    name = "es.mapping.id",
    default = None
  )
  val ES_MAPPING_PARENT = SparkEsConfParam[Option[String]](
    name = "es.mapping.parent",
    default = None
  )
  val ES_MAPPING_VERSION = SparkEsConfParam[Option[String]](
    name = "es.mapping.version",
    default = None
  )
  val ES_MAPPING_VERSION_TYPE = SparkEsConfParam[Option[String]](
    name = "es.mapping.version.type",
    default = None
  )
  val ES_MAPPING_ROUTING = SparkEsConfParam[Option[String]](
    name = "es.mapping.routing",
    default = None
  )
  val ES_MAPPING_TTL_IN_MILLIS = SparkEsConfParam[Option[String]](
    name = "es.mapping.ttl",
    default = None
  )
  val ES_MAPPING_TIMESTAMP = SparkEsConfParam[Option[String]](
    name = "es.mapping.timestamp",
    default = None
  )

  /**
   * Extracts SparkEsMapperConf from a SparkConf.
   *
   * @param sparkConf A SparkConf.
   * @return A SparkEsMapperConf from a SparkConf.
   */
  def fromSparkConf(sparkConf: SparkConf): SparkEsMapperConf = {
    SparkEsMapperConf(
      esMappingId = ES_MAPPING_ID.fromConf(sparkConf)((sc, name) => sc.getOption(name)),
      esMappingParent = ES_MAPPING_PARENT.fromConf(sparkConf)((sc, name) => sc.getOption(name)),
      esMappingVersion = ES_MAPPING_VERSION.fromConf(sparkConf)((sc, name) => sc.getOption(name)),
      esMappingVersionType = ES_MAPPING_VERSION_TYPE.fromConf(sparkConf)((sc, name) => sc.getOption(name)),
      esMappingRouting = ES_MAPPING_ROUTING.fromConf(sparkConf)((sc, name) => sc.getOption(name)),
      esMappingTTLInMillis = ES_MAPPING_TTL_IN_MILLIS.fromConf(sparkConf)((sc, name) => sc.getOption(name)),
      esMappingTimestamp = ES_MAPPING_TIMESTAMP.fromConf(sparkConf)((sc, name) => sc.getOption(name))
    )
  }
}
