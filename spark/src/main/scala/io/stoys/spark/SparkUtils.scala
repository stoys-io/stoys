package io.stoys.spark

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def createSparkSession(sparkConfig: SparkConfig): SparkSession = {
    val builder = SparkSession.builder()
    sparkConfig.spark_options.foreach(kv => builder.config(kv._1, kv._2))
    Option(sparkConfig.master).foreach(builder.master)
    Option(sparkConfig.app_name).foreach(builder.appName)
    builder.getOrCreate()
  }
}
