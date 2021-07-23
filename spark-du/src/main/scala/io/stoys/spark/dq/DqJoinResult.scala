package io.stoys.spark.dq

case class DqJoinInfo(
    left_table_name: String,
    right_table_name: String,
    left_key_column_names: Seq[String],
    right_key_column_names: Seq[String],
    join_type: String,
    join_condition: String
)

case class DqJoinStatistics(
    left_rows: Long,
    right_rows: Long,
    left_nulls: Long,
    right_nulls: Long,
    left_distinct: Long,
    right_distinct: Long,
    inner: Long,
    left: Long,
    right: Long,
    full: Long,
    cross: Long
)

case class DqJoinResult(
    key: String,
    dq_join_info: DqJoinInfo,
    dq_join_statistics: DqJoinStatistics,
    dq_result: DqResult
)
