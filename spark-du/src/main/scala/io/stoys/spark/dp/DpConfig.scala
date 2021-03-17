package io.stoys.spark.dp

case class DpConfig(
    /**
     * What is the maximum length of item we will aggregate (and return)?
     *
     * This helps to ensure the collected items do not run out of memory (for string and binary only).
     */
    max_item_length: Int,

    /**
     * How many unique items should be collected at most?
     *
     * This limit influence number of things:
     * * limits memory requirement of the aggregate function
     *
     * Note: All these items have to fit into memory. The max_item_length can help with that.
     */
    items: Int,

    /**
     * How many buckets will PMF (probability mass function) have?
     *
     * Note: PMF is only computed for numerical types (int, long, float and double) and types sensibly convertible
     * to numbers (date, timestamp and boolean).
     */
    pmf_buckets: Int
)

object DpConfig {
  val default: DpConfig = DpConfig(
    max_item_length = 1024,
    items = 100,
    pmf_buckets = 100
  )
}
