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
     * * unique count [[DpColumn.count_unique]] is exact only if it is lower than this (otherwise it is approximate)
     * * detecting enums (including string booleans) from strings happens only on this many items collected
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
    pmf_buckets: Int,

    /**
     * Time zone id.
     *
     * UTC timezone is used unless no other is set.
     */
    time_zone_id: Option[String],

    /**
     * Infer types (like integer, float or date) for string columns.
     */
    infer_types_from_strings: Boolean,

    /**
     * Type inference config.
     *
     * Ignored if infer_types_from_strings is disabled.
     */
    type_inference_config: DpTypeInferenceConfig,
)

object DpConfig {
  val default: DpConfig = DpConfig(
    max_item_length = 1024,
    items = 100,
    pmf_buckets = 100,
    time_zone_id = Some("UTC"),
    infer_types_from_strings = false,
    type_inference_config = DpTypeInferenceConfig.default,
  )
}
