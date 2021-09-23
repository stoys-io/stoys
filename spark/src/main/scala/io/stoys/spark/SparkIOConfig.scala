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

object SparkIOConfig {
  val default: SparkIOConfig = SparkIOConfig(
    input_paths = Seq.empty,
    input_reshape_config = ReshapeConfig.default,
    register_input_tables = false,
    output_path = None,
    write_format = None,
    write_mode = None,
    write_options = Map.empty
  )
  val notebook: SparkIOConfig = default.copy(
    input_reshape_config = ReshapeConfig.notebook,
    register_input_tables = true,
    write_mode = Some("overwrite")
  )
}
