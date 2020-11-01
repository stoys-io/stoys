package io.stoys.core

import io.stoys.core.util.Params

@Params(allowInRootPackage = true)
case class SparkConfig(
    appName: String,
    master: String,
    // sparkOptions are options for SparkSession.Builder
    sparkOptions: Map[String, String]
)
