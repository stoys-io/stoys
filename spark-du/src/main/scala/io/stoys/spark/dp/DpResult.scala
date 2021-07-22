package io.stoys.spark.dp

case class DpPmfBucket(
    low: Double,
    high: Double,
    count: Long
)

case class DpItem(
    item: String,
    count: Long
)

case class DpColumn(
    name: String,
    data_type: String,
    data_type_json: String,
    nullable: Boolean,
    enum_values: Seq[String],
    format: Option[String],
    count: Long,
    count_empty: Option[Long],
    count_nulls: Option[Long],
    count_unique: Option[Long],
    count_zeros: Option[Long],
    max_length: Option[Long],
    min: Option[String],
    max: Option[String],
    mean: Option[Double],
    pmf: Seq[DpPmfBucket],
    items: Seq[DpItem],
    extras: Map[String, String]
    // TODO: percentiles, stddev?
)

case class DpTable(
    rows: Long
)

case class DpResult(
    table: DpTable,
    columns: Seq[DpColumn]
)
