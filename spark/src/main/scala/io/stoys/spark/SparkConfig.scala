package io.stoys.spark

import io.stoys.scala.Params

@Params(allowInRootPackage = true)
case class SparkConfig(
    app_name: String,
    master: String,
    // spark_options are options for SparkSession.Builder
    spark_options: Map[String, String],
)
