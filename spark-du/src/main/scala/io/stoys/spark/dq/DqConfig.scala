package io.stoys.spark.dq

case class DqConfig(
    sample_rows: Boolean,
    max_rows_per_rule: Int,
    max_rows: Int,
    fail_on_extra_columns: Boolean
)

object DqConfig {
  val default: DqConfig = DqConfig(
    sample_rows = true,
    max_rows_per_rule = 20,
    max_rows = 1000,
    fail_on_extra_columns = false
  )
}
