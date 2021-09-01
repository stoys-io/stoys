package io.stoys.spark

import io.stoys.scala.Strings
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
    coerceTypes: Boolean,
    /**
     * It behaves as [[ReshapeConflictResolution.ERROR]] regardless of the config (for now).
     *
     * What should happen when two conflicting column definitions are encountered?
     *
     * In particular the value [[ReshapeConflictResolution.LAST]] is useful. It solves the common pattern
     * of conflicting columns occurring in queries like "SELECT *, "some_value" AS value FROM table" where "table"
     * already has "value".
     */
    conflictResolution: ReshapeConflictResolution,
    /**
     * Should we drop the extra columns (not present in target schema)?
     *
     * Use false to keep the extra columns (just like [[Dataset.as]] does).
     */
    dropExtraColumns: Boolean,
    /**
     * Should we fail on presence of extra column (not present in target schema)?
     */
    failOnExtraColumn: Boolean,
    /**
     * Should we fail on nullable source field being assigned to non nullable target field?
     *
     * Note: It is useful to ignore nullability by default (just like [[Dataset.as]] does). Spark inferred type
     * for every json and csv field as nullable. This is true even if there is not a single null in the dataset.
     * All parquet files written by spark also have every field nullable for compatibility.
     */
    failOnIgnoringNullability: Boolean,
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
    fillDefaultValues: Boolean,
    /**
     * Should we fill nulls instead of missing nullable columns?
     */
    fillMissingNulls: Boolean,
    /**
     * Should indexed based mapping be used?
     *
     * Index based mapping ignores source field names. It assume source fields map to target fields in given order.
     *
     * Note:  This is good for example for csv files without headers.
     *
     * BEWARE: This is quite error prone for production code if there is any change source type will change.
     * The changes in source structure may lead to wrong field mapping without any errors.
     */
    indexBasedMatching: Boolean,
    /**
     * Should names be normalized before matching?
     *
     * Number of normalizations happen - trim, lowercase, replace non-word characters with underscores, etc.
     * For details see [[Strings.toSnakeCase]].
     *
     * Note: Trim (drop leading and trailing spaces) and lower casing happens even when this is disabled!
     */
    normalizedNameMatching: Boolean,
    /**
     * How should the output columns be sorted?
     *
     * Use [[ReshapeSortOrder.TARGET]] to get the order of target schema.
     */
    sortOrder: ReshapeSortOrder,
    /**
     * Should we use custom date format string?
     */
    dateFormat: Option[String],
    /**
     * Should we use custom timestamp format string?
     */
    timestampFormat: Option[String]
)

object ReshapeConfig {
  /**
   * [[ReshapeConfig.as]] behaves the same way as Spark's own [[Dataset.as]].
   */
  val as: ReshapeConfig = ReshapeConfig(
    coerceTypes = false,
    conflictResolution = ReshapeConflictResolution.ERROR,
    dropExtraColumns = false,
    failOnExtraColumn = false,
    failOnIgnoringNullability = false,
    fillDefaultValues = false,
    fillMissingNulls = false,
    indexBasedMatching = false,
    normalizedNameMatching = false,
    sortOrder = ReshapeSortOrder.SOURCE,
    dateFormat = None,
    timestampFormat = None
  )
  val safe: ReshapeConfig = as.copy(
    failOnExtraColumn = true,
    failOnIgnoringNullability = true
  )
  val default: ReshapeConfig = as.copy(
    coerceTypes = true,
    dropExtraColumns = true,
    sortOrder = ReshapeSortOrder.TARGET
  )
  val dangerous: ReshapeConfig = default.copy(
    conflictResolution = ReshapeConflictResolution.LAST,
    fillDefaultValues = true,
    fillMissingNulls = true,
    normalizedNameMatching = true
  )
}
