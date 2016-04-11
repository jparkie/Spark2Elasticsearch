package com.github.jparkie.spark.elasticsearch.util

import org.apache.spark.SparkConf

/**
 * Defines parameter to extract values from SparkConf.
 *
 * @param name The key in SparkConf.
 * @param default The default value to fallback on missing key.
 */
case class SparkEsConfParam[T](name: String, default: T) {
  def fromConf(sparkConf: SparkConf)(sparkConfFunc: (SparkConf, String) => T): T = {
    if (sparkConf.contains(name)) {
      sparkConfFunc(sparkConf, name)
    } else if (sparkConf.contains(s"spark.$name")) {
      sparkConfFunc(sparkConf, s"spark.$name")
    } else {
      default
    }
  }
}
