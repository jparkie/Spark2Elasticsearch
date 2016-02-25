package com.github.jparkie.spark.elasticsearch

import org.apache.spark.sql.DataFrame

package object sql {
  /**
   * Implicitly lift a DataFrame with SparkEsDataFrameFunctions.
   *
   * @param dataFrame A DataFrame to lift.
   * @return Enriched DataFrame with SparkEsDataFrameFunctions.
   */
  implicit def sparkEsDataFrameFunctions(dataFrame: DataFrame): SparkEsDataFrameFunctions = new SparkEsDataFrameFunctions(dataFrame)
}
