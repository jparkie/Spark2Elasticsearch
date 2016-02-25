package com.github.jparkie.spark.elasticsearch

/**
 * Serializes a T into an Array[Byte] for an IndexRequest.
 *
 * @tparam T T Object to serialize.
 */
trait SparkEsSerializer[T] extends Serializable {
  /**
   * Serialize a T from a DataFrame into an Array[Byte].
   *
   * @param value A T
   * @return The source T as Array[Byte].
   */
  def write(value: T): Array[Byte]
}
