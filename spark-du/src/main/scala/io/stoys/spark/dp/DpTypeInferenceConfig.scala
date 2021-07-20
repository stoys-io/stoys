package io.stoys.spark.dp

import java.time.format.DateTimeFormatter

case class DpTypeInferenceConfig(
    /**
     * Is it preferable to mark column as not nullable no observed value is null?
     *
     * Everything will be inferred as nullable if this is disabled (even if no null is observed for a column).
     */
    prefer_not_nullable: Boolean,

    /**
     * Is it preferable to infer boolean when all observed values are "0" or "1" or both?
     *
     * Otherwise the column will be inferred as integer or long.
     */
    prefer_boolean_for_01: Boolean,

    /**
     * Is it preferable to infer float (instead of double) when all observed values are within float range
     * and have less than 10 digits?
     */
    prefer_float: Boolean,

    /**
     * Is it preferable to infer integer (instead of long) when all observed values fit within integer range?
     */
    prefer_integer: Boolean,

    /**
     * Allowed date formats.
     *
     * See [[DateTimeFormatter]] for the string format.
     */
    date_formats: Seq[String],

    /**
     * Allowed timestamp formats.
     *
     * See [[DateTimeFormatter]] for the string format.
     */
    timestamp_formats: Seq[String],

    /**
     * Allowed enums (their names and values).
     *
     * EnumValue has a name and a list of values (string literals). The name should be class name (without package)
     * of the enum class (if there is one). Multiple records with the same name and different sets of values may exist.
     * In that case only one (the first) match will be returned. The order of values matter! They are zero indexed.
     * It matters for conversion between string and number as well as it does for min, max, mean, etc.
     *
     * Then name "Boolean" is special! It expect exactly two values (for false and true value) and infers BooleanType.
     * The values for boolean can be string literals like Seq("no", "yes") or Seq("N", "Y"). Numbers like Seq("0", "1")
     * will not work this way for boolean. Those will be inferred as an integer unless prefer_boolean_for_01 is enabled.
     *
     * {{{
     * Seq(
     *   EnumValues("Boolean", Seq("N", "Y")),
     *   EnumValues("Boolean", Seq("false", "true")),
     *   EnumValues("SexEnum", Seq("UNKNOWN", "MALE", "FEMALE", "NON_BINARY")
     * )
     * }}}
     */
    enum_values: Seq[DpTypeInferenceConfig.EnumValues]
)

object DpTypeInferenceConfig {
  val default: DpTypeInferenceConfig = DpTypeInferenceConfig(
    prefer_not_nullable = false,
    prefer_boolean_for_01 = false,
    prefer_float = false,
    prefer_integer = false,
    date_formats = Seq("yyyy-MM-dd", "MM/dd/yyyy", "dd.MM.yyyy"),
    timestamp_formats = Seq("yyyy-MM-dd HH:mm:ss"),
    enum_values = Seq(
      EnumValues(EnumValues.BOOLEAN_NAME, Seq("FALSE", "TRUE")),
      EnumValues(EnumValues.BOOLEAN_NAME, Seq("NO", "YES")),
      EnumValues(EnumValues.BOOLEAN_NAME, Seq("N", "Y"))
    )
  )

  case class EnumValues(
      name: String,
      values: Seq[String]
  )

  object EnumValues {
    val BOOLEAN_NAME = "Boolean"
  }
}
