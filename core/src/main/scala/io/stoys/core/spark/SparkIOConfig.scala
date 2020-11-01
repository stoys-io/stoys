package io.stoys.core.spark

import io.stoys.core.util.Params

@Params(allowInRootPackage = true)
case class SparkIOConfig(
    inputPaths: Seq[String],
    outputPath: Option[String],
    writeFormat: Option[String],
    writeMode: Option[String],
    writeOptions: Map[String, String]
)
