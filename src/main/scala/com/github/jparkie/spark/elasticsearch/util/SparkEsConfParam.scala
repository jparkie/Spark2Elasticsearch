package com.github.jparkie.spark.elasticsearch.util

/**
 * Defines parameter to extract values from SparkConf.
 *
 * @param name The key in SparkConf.
 * @param default The default value to fallback on missing key.
 */
case class SparkEsConfParam[T](name: String, default: T)
