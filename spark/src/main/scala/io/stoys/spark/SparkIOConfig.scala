package io.stoys.spark

import io.stoys.scala.Params

@Params(allowInRootPackage = true)
case class SparkIOConfig(
    input_paths: Seq[String],
    input_reshape_config: ReshapeConfig,
    register_input_tables: Boolean,
    output_path: Option[String],
    write_format: Option[String],
    write_mode: Option[String],
    write_options: Map[String, String]
)
