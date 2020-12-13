package io.stoys.spark.dq

import java.util.Locale

import io.stoys.spark.dq.DqRules._
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable

object DqSchema {
  def generateSchemaRules(existingSchema: StructType, expectedFields: Seq[DqField], primaryKeyFieldNames: Seq[String],
      config: DqConfig): Seq[DqRule] = {
    val rules = mutable.Buffer.empty[DqRule]
    val existingFiledNames = existingSchema.map(_.name.toLowerCase(Locale.ROOT))
    val expectedFiledNames = expectedFields.map(_.name.toLowerCase(Locale.ROOT))
    val missingFieldNames = expectedFiledNames.filterNot(existingFiledNames.toSet)
    if (missingFieldNames.nonEmpty) {
      val description = s"Expected fields should: ${missingFieldNames.mkString(", ")}"
      val rule = namedRule("_expected_fields", "exist", "false", description)
      rules += rule.copy(referenced_column_names = missingFieldNames)
    }
    if (config.report_extra_columns) {
      val extraFieldNames = existingFiledNames.filterNot(expectedFiledNames.toSet)
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
        rules += typeRule(fieldName, existingField.dataType, expectedType)
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
