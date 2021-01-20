package io.stoys.spark.dq

case class DqJoinResult(
    join_statistics: DqJoinStatistics,
    dq_result: DqResult
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
