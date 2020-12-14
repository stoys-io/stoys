package io.stoys.spark.dq

import java.util.Locale

import io.stoys.spark.dq.DqRules._
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable

private[dq] object DqSchema {
  def generateSchemaRules(existingSchema: StructType, expectedFields: Seq[DqField], primaryKeyFieldNames: Seq[String],
      config: DqConfig): Seq[DqRule] = {
    val rules = mutable.Buffer.empty[DqRule]
    val existingFieldNames = existingSchema.map(_.name.toLowerCase(Locale.ROOT))
    val expectedFieldNames = expectedFields.map(_.name.toLowerCase(Locale.ROOT))
    val missingFieldNames = expectedFieldNames.filterNot(existingFieldNames.toSet)
    if (missingFieldNames.nonEmpty) {
      val description = s"Expected fields should: ${missingFieldNames.mkString(", ")}"
      val rule = namedRule("_expected_fields", "exist", "false", description)
      rules += rule.copy(referenced_column_names = missingFieldNames)
    }
    if (config.report_extra_columns) {
      val extraFieldNames = existingFieldNames.filterNot(expectedFieldNames.toSet)
      val description = s"Extra fields should not exist: ${extraFieldNames.mkString(", ")}"
      rules += namedRule("_extra_fields", "not_exist", "false", description)
    }
    if (primaryKeyFieldNames.nonEmpty) {
      val pkFieldName = "_primary_key"
      rules += namedRule(pkFieldName, "not_null", primaryKeyFieldNames.map(fn => s"$fn IS NOT NULL").mkString(" AND "))
      rules += uniqueRule(pkFieldName, primaryKeyFieldNames)
    }
    val existingFieldsByName = existingSchema.map(f => f.name.toLowerCase(Locale.ROOT) -> f).toMap
    expectedFields.foreach { expectedField =>
      existingFieldsByName.get(expectedField.name.toLowerCase(Locale.ROOT)).foreach { existingField =>
        val fieldName = expectedField.name
        val expectedType = DataType.fromJson('"' + expectedField.typ + '"')
        rules += typeRule(fieldName, existingField.dataType, expectedType, format = expectedField.format.orNull)
        if (!expectedField.nullable) {
          rules += notNullRule(fieldName)
        }
        if (expectedField.enum_values.nonEmpty) {
          rules += enumValuesRule(fieldName, expectedField.enum_values)
        }
        expectedField.regexp.foreach { regexp =>
          rules += regexpRule(fieldName, regexp)
        }
      }
    }
    rules
  }
}
