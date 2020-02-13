package com.nuna.trustdb.core.spark

import com.nuna.trustdb.core.SparkConfig
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def createSparkSession(sparkConfig: SparkConfig): SparkSession = {
    val builder = SparkSession.builder()
    sparkConfig.sparkOptions.foreach(kv => builder.config(kv._1, kv._2))
    Option(sparkConfig.master).foreach(builder.master)
    Option(sparkConfig.appName).foreach(builder.appName)
    val session = builder.getOrCreate()
    session.sparkContext.setLogLevel(sparkConfig.logLevel)
    session
  }
}
