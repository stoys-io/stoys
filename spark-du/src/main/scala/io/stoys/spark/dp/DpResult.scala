package io.stoys.spark.dp

case class DpBucketCount[T](low: T, high: T, count: Long)

case class DpItemCount[T](item: T, count: Long)

case class DpColumn[T](
    name: String,
    data_type: String,
    count: Long,
    count_empty: Option[Long],
    count_nulls: Option[Long],
    count_unique: Option[Long],
    count_zeros: Option[Long],
    max_length: Option[Long],
    min: Option[T],
    max: Option[T],
    mean: Option[T],
    histogram: Seq[DpBucketCount[Float]],
    items: Seq[DpItemCount[T]]
    // TODO: percentiles, is_typ_inferred, stddev?
)

case class DpTable(
    rows: Long
)

case class DpResult(
    table: DpTable,
    columns: Seq[DpColumn[String]]
)
