package io.stoys.spark.dq

import io.stoys.spark.SToysException
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast}
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}

object DqRules {
  private val LOGICAL_NAME_SEPARATOR = "__"

  // factories for case classes

  def name(fieldName: String, logicalName: String): String = {
    s"$fieldName$LOGICAL_NAME_SEPARATOR$logicalName"
  }

  def namedRule(fieldName: String, logicalName: String, expression: String, description: String = null): DqRule = {
    rule(name(fieldName, logicalName), expression, description)
  }

  def nullSafeNamedRule(
      fieldName: String, logicalName: String, expression: String, description: String = null): DqRule = {
    namedRule(fieldName, logicalName, s"`$fieldName` IS NULL OR ($expression)", description)
  }

  def rule(name: String, expression: String, description: String = null): DqRule = {
    DqRule(name, expression, Option(description), Seq.empty)
  }

  def rule(alias: Alias): DqRule = {
    rule(alias, null)
  }

  def rule(alias: Alias, description: String): DqRule = {
    rule(alias.name, alias.child.sql, description)
  }

  def rule(column: Column): DqRule = {
    rule(column, null)
  }

  def rule(column: Column, description: String): DqRule = {
    column.expr match {
      case a: Alias => rule(a, description)
      case _ => throw new SToysException("Column is missing name. Add it with '.as(...)' function.`")
    }
  }

  def field(name: String, dataTypeJson: String, nullable: Boolean = true, enumValues: Seq[String] = Seq.empty,
      format: String = null, regexp: String = null): DqField = {
    DqField(name, dataTypeJson, nullable, enumValues, Option(format), Option(regexp))
  }

  // common rules

  def enumValuesRule(fieldName: String, enumValues: Seq[String], caseInsensitive: Boolean = false): DqRule = {
    val expression = if (caseInsensitive) {
      s"UPPER(CAST(`$fieldName` AS STRING)) IN ${enumValues.map(_.toUpperCase()).mkString("('", "', '", "')")}"
    } else {
      s"CAST(`$fieldName` AS STRING) IN ${enumValues.mkString("('", "', '", "')")}"
    }
    nullSafeNamedRule(fieldName, "enum_values", expression)
  }

  def notNullRule(fieldName: String): DqRule = {
    namedRule(fieldName, "not_null", s"`$fieldName` IS NOT NULL")
  }

  def regexpRule(fieldName: String, regexp: String): DqRule = {
    nullSafeNamedRule(fieldName, "regexp", s"CAST(`$fieldName` AS STRING) RLIKE '$regexp'")
  }

  def typeRule(fieldName: String, sourceType: DataType, targetType: DataType, format: String = null): DqRule = {
    if (Cast.canCast(sourceType, targetType)) {
      val expression = (targetType, Option(format)) match {
        case (DateType, Some(format)) => s"TO_DATE(`$fieldName`, '$format') IS NOT NULL"
        case (TimestampType, Some(format)) => s"TO_TIMESTAMP(`$fieldName`, '$format') IS NOT NULL"
        case (dataType, _) => s"CAST(`$fieldName` AS ${dataType.sql}) IS NOT NULL"
      }
      nullSafeNamedRule(fieldName, "type", expression)
    } else {
      val description = s"Cannot cast `$fieldName` from '$sourceType' to '$targetType'."
      namedRule(fieldName, "type", "false", description)
    }
  }

  def uniqueRule(fieldName: String): DqRule = {
    uniqueRule(fieldName, Seq(fieldName))
  }

  // composite and multi field rules

  def all(ruleName: String, rules: Seq[DqRule], description: String = null): DqRule = {
    rule(ruleName, rules.map(_.expression).mkString("(", ") AND (", ")"), description)
  }

  def any(ruleName: String, rules: Seq[DqRule], description: String = null): DqRule = {
    rule(ruleName, rules.map(_.expression).mkString("(", ") OR (", ")"), description)
  }

  def uniqueRule(baseRuleName: String, fieldNames: Seq[String]): DqRule = {
    namedRule(baseRuleName, "unique", s"(COUNT(*) OVER (PARTITION BY ${fieldNames.mkString("`", "`, `", "`")})) = 1")
  }
}
