package io.stoys.spark

import org.apache.spark.sql.Dataset

case class ReshapeConfig(
    /**
     * Should primitive types be coerced?
     *
     * If enabled it will automatically do the usual upcasting like [[Int]] to [[Long]] but not the other way around
     * (as it would loose precision).
     *
     * BEWARE: Everything can be casted implicitly to [[String]]! But often it is not desirable.
     */
    coerce_types: Boolean,
    /**
     * It behaves as [[ReshapeConflictResolution.ERROR]] regardless of the config (for now).
     *
     * What should happen when two conflicting column definitions are encountered?
     *
     * In particular the value [[ReshapeConflictResolution.LAST]] is useful. It solves the common pattern
     * of conflicting columns occurring in queries like "SELECT *, "some_value" AS value FROM table" where "table"
     * already has "value".
     */
    conflict_resolution: ReshapeConflictResolution,
    /**
     * Should we drop the extra columns (not present in target schema)?
     *
     * Use false to keep the extra columns (just like [[Dataset.as]] does).
     */
    drop_extra_columns: Boolean,
    /**
     * Should we fail on presence of extra column (not present in target schema)?
     */
    fail_on_extra_column: Boolean,
    /**
     * Should we fail on nullable source field being assigned to non nullable target field?
     *
     * Note: It is useful to ignore nullability by default (just like [[Dataset.as]] does). Spark inferred type
     * for every json and csv field as nullable. This is true even if there is not a single null in the dataset.
     * All parquet files written by spark also have every field nullable for compatibility.
     */
    fail_on_ignoring_nullability: Boolean,
    /**
     * Should we fill in default values instead of nulls for non nullable columns?
     *
     * Default values are 0 for numbers, false for [[Boolean]], "" for [[String]], empty arrays and nested struct
     * are present filled with the previous rules.
     *
     * Note: It probably does not make sense to use this without also setting fillMissingNulls = true.
     *
     * BEWARE: This is not intended for production code! Filling blindly all columns with default values defeats
     * the purpose of why they are not defined nullable in the first place. It would be error prone to use this.
     * But it may come handy in some notebook exploration or unit tests.
     */
    fill_default_values: Boolean,
    /**
     * Should we fill nulls instead of missing nullable columns?
     */
    fill_missing_nulls: Boolean,
    /**
     * Which field matching strategy should be used?
     */
    field_matching_strategy: ReshapeFieldMatchingStrategy,
    /**
     * How should the output columns be sorted?
     *
     * Use [[ReshapeSortOrder.TARGET]] to get the order of target schema.
     */
    sort_order: ReshapeSortOrder,
    /**
     * Should we use custom date format string?
     */
    date_format: Option[String],
    /**
     * Should we use custom timestamp format string?
     */
    timestamp_format: Option[String],
)

object ReshapeConfig {
  /**
   * [[ReshapeConfig.spark]] behaves the same way as Spark's own [[Dataset.as]].
   */
  val spark: ReshapeConfig = ReshapeConfig(
    coerce_types = false,
    conflict_resolution = ReshapeConflictResolution.ERROR,
    drop_extra_columns = false,
    fail_on_extra_column = false,
    fail_on_ignoring_nullability = false,
    fill_default_values = false,
    fill_missing_nulls = false,
    field_matching_strategy = ReshapeFieldMatchingStrategy.NAME_DEFAULT,
    sort_order = ReshapeSortOrder.SOURCE,
    date_format = None,
    timestamp_format = None,
  )
  val paranoid: ReshapeConfig = spark.copy(
    fail_on_extra_column = true,
    fail_on_ignoring_nullability = true,
  )
  val default: ReshapeConfig = spark.copy(
    coerce_types = true,
    drop_extra_columns = true,
    sort_order = ReshapeSortOrder.TARGET,
  )
  val dangerous: ReshapeConfig = default.copy(
    conflict_resolution = ReshapeConflictResolution.LAST,
    fill_default_values = true,
    fill_missing_nulls = true,
    field_matching_strategy = ReshapeFieldMatchingStrategy.NAME_NORMALIZED,
  )
  val notebook: ReshapeConfig = spark.copy(
    coerce_types = true,
    conflict_resolution = ReshapeConflictResolution.LAST,
    fill_missing_nulls = true,
    field_matching_strategy = ReshapeFieldMatchingStrategy.NAME_NORMALIZED,
  )
}
