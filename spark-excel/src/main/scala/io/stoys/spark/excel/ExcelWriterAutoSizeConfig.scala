package io.stoys.spark.excel

case class ExcelWriterAutoSizeConfig(
    max_column_name_width: Int,
    min_column_width: Int,
    max_column_width: Int,
    scaling_factor: Double
)

object ExcelWriterAutoSizeConfig {
  val default: ExcelWriterAutoSizeConfig = ExcelWriterAutoSizeConfig(
    max_column_name_width = 12,
    min_column_width = 4,
    max_column_width = 64,
    scaling_factor = 1.4
  )
}
