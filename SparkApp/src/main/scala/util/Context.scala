package util

import org.apache.spark.sql.SparkSession

trait Context {
  lazy val spark=SparkSession.builder().appName("Spark App").master("local[*]").getOrCreate()
}