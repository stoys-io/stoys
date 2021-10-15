package io.stoys.spark.aggsum

case class AggSumInfo(
    key_column_names: Seq[String],
    value_column_names: Seq[String],
    referenced_table_names: Seq[String],
)

case class AggSumResult(
    aggsum_info: AggSumInfo,
    data_json: String,
)
