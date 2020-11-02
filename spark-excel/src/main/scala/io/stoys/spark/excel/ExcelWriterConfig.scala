package io.stoys.spark.excel

case class ExcelWriterConfig(
    use_header: Boolean,
    auto_size_columns: Boolean,
    auto_size_config: ExcelWriterAutoSizeConfig,
    convert_cents_to_currency: Boolean,
    convert_unsupported_types_to_string: Boolean,
    currency_format: String,
    date_format: String,
    timestamp_format: String,
    template_xlsx: Array[Byte],
    poi_based_auto_size_columns: Boolean,
    poi_streaming_row_access_window_size: Option[Int]
)

object ExcelWriterConfig {
  val default: ExcelWriterConfig = ExcelWriterConfig(
    use_header = true,
    auto_size_columns = true,
    auto_size_config = ExcelWriterAutoSizeConfig.default,
    convert_cents_to_currency = true,
    convert_unsupported_types_to_string = false,
    currency_format = "$#,##0_);($#,##0)",
    date_format = "M/D/YYYY",
    timestamp_format = "dd-mm-yyyy hh:mm:ss",
    template_xlsx = null,
    poi_based_auto_size_columns = false,
    poi_streaming_row_access_window_size = None
  )
}
