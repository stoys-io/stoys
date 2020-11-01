package io.stoys.spark

import io.stoys.scala.Params

@Params(allowInRootPackage = true)
case class SparkConfig(
    appName: String,
    master: String,
    // sparkOptions are options for SparkSession.Builder
    sparkOptions: Map[String, String]
)
