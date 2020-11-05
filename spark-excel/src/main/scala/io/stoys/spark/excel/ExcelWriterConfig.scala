package io.stoys.spark.excel

case class ExcelWriterConfig(
    /**
     * Should column names be written as a first row of the table?
     */
    use_header: Boolean,
    /**
     * Should the columns be resized? Columns with wide headers or values can become wider.
     */
    auto_size_columns: Boolean,
    /**
     * Auto sizing configuration. See [[ExcelWriterAutoSizeConfig]] for details.
     *
     * Note: This works for uot implementation of auto sizing. Ignored when [[poi_based_auto_size_columns]] is true.
     */
    auto_size_config: ExcelWriterAutoSizeConfig,
    /**
     * Should we convert columns of [[org.apache.spark.sql.types.LongType]] with name ending '_cents'
     * into monetary value divided by 100.0?
     */
    convert_cents_to_currency: Boolean,
    /**
     * Should unsupported value types be written as [[String]] (after calling [[AnyRef.toString()]] on them?
     *
     * Note: Only a few types are supported by default - jvm primitives, [[String]], [[java.util.Date]]
     * (and a few other temporal types). See [[ExcelWriter]] implementation for details.
     */
    convert_unsupported_types_to_string: Boolean,
    /**
     * Excel style currency format string.
     */
    currency_format: String,
    /**
     * Excel style date format string.
     */
    date_format: String,
    /**
     * Excel style timestamp format string.
     */
    timestamp_format: String,
    /**
     * Template xlsx file to use. See unit tests or complementary documentation for examples.
     *
     * Note: It is required as [[Array[Byte]]] to avoid file availability and permission issues in cloud deployments.
     */
    template_xlsx: Array[Byte],
    /**
     * Should we use Apache POI implementation of column auto sizing?
     */
    @deprecated("POI internal resizing is nice idea. But it is exceptionally slow. Nobody should really use it.")
    poi_based_auto_size_columns: Boolean,
    /**
     * Apache POI library used to generate excel files is very memory hungry (and slow). Streaming should help with
     * that by writing file parts to local filesystem. See apache poi documentation for details.
     */
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
