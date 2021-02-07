package io.stoys.spark

import java.util.ServiceLoader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.DataSourceRegister

import scala.jdk.CollectionConverters._

object SparkUtils {
  def createSparkSession(sparkConfig: SparkConfig): SparkSession = {
    val builder = SparkSession.builder()
    sparkConfig.sparkOptions.foreach(kv => builder.config(kv._1, kv._2))
    Option(sparkConfig.master).foreach(builder.master)
    Option(sparkConfig.appName).foreach(builder.appName)
    builder.getOrCreate()
  }

  def isDeltaSupported: Boolean = {
    ServiceLoader.load(classOf[DataSourceRegister]).iterator().asScala.exists(_.shortName().toLowerCase() == "delta")
  }
}
