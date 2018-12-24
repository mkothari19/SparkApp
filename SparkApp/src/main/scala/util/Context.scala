package util

import org.apache.spark.sql.SparkSession
/*
 * Bootstrap spark session
 */
trait Context {
  lazy val spark=SparkSession.builder().appName("Spark App").master("local[*]").getOrCreate()
}