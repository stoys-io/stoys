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
     * NOT IMPLEMENTED YET!!!
     * It behaves as [[ReshapeConfig.ConflictResolution.ERROR]] regardless of the config (for now).
     *
     * What should happen when two conflicting column definitions are encountered?
     *
     * In particular the value [[ReshapeConfig.ConflictResolution.LAST]] is useful. It solves the common pattern
     * of conflicting columns occurring in queries like "SELECT *, "some_value" AS value FROM table" where "table"
     * already has "value".
     */
    conflictResolution: ReshapeConfig.ConflictResolution.ConflictingNameResolution,
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
     * Use [[ReshapeConfig.SortOrder.TARGET]] to get the order of target schema.
     */
    sortOrder: ReshapeConfig.SortOrder.SortOrder,
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
  object ConflictResolution extends Enumeration {
    type ConflictingNameResolution = Value
    val UNDEFINED, ERROR, FIRST, LAST = Value
  }

  object SortOrder extends Enumeration {
    type SortOrder = Value
    val UNDEFINED, ALPHABETICAL, SOURCE, TARGET = Value
  }

  /**
   * [[ReshapeConfig.as]] behaves the same way as Spark's own [[Dataset.as]].
   */
  val as: ReshapeConfig = ReshapeConfig(
    coerceTypes = false,
    conflictResolution = ReshapeConfig.ConflictResolution.ERROR,
    dropExtraColumns = false,
    failOnExtraColumn = false,
    failOnIgnoringNullability = false,
    fillDefaultValues = false,
    fillMissingNulls = false,
    normalizedNameMatching = false,
    sortOrder = ReshapeConfig.SortOrder.SOURCE,
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
    sortOrder = ReshapeConfig.SortOrder.TARGET
  )
  val dangerous: ReshapeConfig = default.copy(
    conflictResolution = ReshapeConfig.ConflictResolution.LAST,
    fillDefaultValues = true,
    fillMissingNulls = true,
    normalizedNameMatching = true
  )
}
