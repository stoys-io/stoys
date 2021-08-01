package io.stoys.spark.dq

case class DqConfig(
    /**
     * Should [[DqResult.row_sample]] output be enabled?
     */
    sample_rows: Boolean,

    /**
     * How many rows (at most) are sampled per rule?
     *
     * Note: The actual limit may be proportionally lowered if max_rows limit is reached.
     */
    max_rows_per_rule: Int,

    /**
     * How many rows are sampled across all rules?
     */
    max_rows: Int,

    /**
     * Should [[DqSchema]] generate rule failing (and listing) extra columns?
     *
     * Extra columns are silently ignored if this disabled.
     */
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
