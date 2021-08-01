package io.stoys.spark.dq

import io.stoys.spark.dq.DqRules._
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable

private[dq] object DqSchema {
  def generateSchemaRules(existingSchema: StructType, expectedFields: Seq[DqField], primaryKeyFieldNames: Seq[String],
      config: DqConfig): Seq[DqRule] = {
    val rules = mutable.Buffer.empty[DqRule]
    val existingFieldNames = existingSchema.map(_.name.toLowerCase)
    val expectedFieldNames = expectedFields.map(_.name.toLowerCase)
    val missingFieldNames = expectedFieldNames.filterNot(existingFieldNames.toSet)
    val expectedPrimaryKeyFieldNames = primaryKeyFieldNames.map(_.toLowerCase)
    val missingPrimaryKeyFieldNames = expectedPrimaryKeyFieldNames.filterNot(existingFieldNames.toSet)
    val allMissingFieldNames = missingFieldNames ++ missingPrimaryKeyFieldNames.filterNot(missingFieldNames.toSet)
    if (allMissingFieldNames.nonEmpty) {
      val description = s"Expected fields should exist: ${allMissingFieldNames.mkString(", ")}"
      val rule = namedRule("_expected_fields", "exist", "false", description)
      rules += rule.copy(referenced_column_names = allMissingFieldNames)
    }
    if (config.report_extra_columns) {
      val extraFieldNames = existingFieldNames.filterNot(expectedFieldNames.toSet)
      val description = s"Extra fields should not exist: ${extraFieldNames.mkString(", ")}"
      rules += namedRule("_extra_fields", "not_exist", "false", description)
    }
    if (expectedPrimaryKeyFieldNames.nonEmpty && missingPrimaryKeyFieldNames.isEmpty) {
      val primaryKeyNotNullExpr = expectedPrimaryKeyFieldNames.map(fn => s"`$fn` IS NOT NULL").mkString(" AND ")
      rules += namedRule("_primary_key", "not_null", primaryKeyNotNullExpr)
      rules += uniqueRule("_primary_key", expectedPrimaryKeyFieldNames)
    }
    val existingFieldsByName = existingSchema.map(f => f.name.toLowerCase -> f).toMap
    expectedFields.foreach { expectedField =>
      existingFieldsByName.get(expectedField.name.toLowerCase).foreach { existingField =>
        val fieldName = expectedField.name
        Option(expectedField.data_type_json).foreach { dataTypeJson =>
          val format = Option(expectedField.format).flatten.orNull
          rules += typeRule(fieldName, existingField.dataType, DataType.fromJson(dataTypeJson), format)
        }
        if (!expectedField.nullable) {
          rules += notNullRule(fieldName)
        }
        if (Option(expectedField.enum_values).exists(_.nonEmpty)) {
          rules += enumValuesRule(fieldName, expectedField.enum_values)
        }
        Option(expectedField.regexp).flatten.foreach { regexp =>
          rules += regexpRule(fieldName, regexp)
        }
      }
    }
    rules.toSeq
  }
}
