package io.stoys.spark

import io.stoys.scala.Params

@Params(allowInRootPackage = true)
case class SparkIOConfig(
    inputPaths: Seq[String],
    inputReshapeConfig: ReshapeConfig,
    outputPath: Option[String],
    writeFormat: Option[String],
    writeMode: Option[String],
    writeOptions: Map[String, String]
)
