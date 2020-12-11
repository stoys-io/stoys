package io.stoys.spark.dq

import io.stoys.spark.SToysException
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast}
import org.apache.spark.sql.types.DataType

object DqRules {
  private val LOGICAL_NAME_SEPARATOR = "__"

  // factories for case classes

  def name(fieldName: String, logicalName: String): String  = {
    s"$fieldName$LOGICAL_NAME_SEPARATOR$logicalName"
  }

  def namedRule(fieldName: String, logicalName: String, expression: String, description: String = null): DqRule = {
    rule(name(fieldName, logicalName), expression, description)
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

  def field(name: String, typ: String, nullable: Boolean = true, enumValues: Seq[String] = Seq.empty,
      regex: String = null): DqField = {
    DqField(name, typ, nullable, enumValues, Option(regex))
  }

  // common rules

  def enumValuesRule(fieldName: String, enumValues: Seq[String], caseInsensitive: Boolean = false): DqRule = {
    val expression = if (caseInsensitive) {
      s"UPPER(CAST($fieldName AS STRING)) IN ${enumValues.map(_.toUpperCase()).mkString("('", "', '", "')")}"
    } else {
      s"CAST($fieldName AS STRING) IN ${enumValues.mkString("('", "', '", "')")}"
    }
    namedRule(fieldName, "enum_values", expression)
  }

  def notNullRule(fieldName: String): DqRule = {
    namedRule(fieldName, "not_null", s"$fieldName IS NOT NULL")
  }

  def regexRule(fieldName: String, regex: String): DqRule = {
    namedRule(fieldName, "regex", s"CAST($fieldName AS STRING) RLIKE '$regex'")
  }

  def typeRule(fieldName: String, sourceType: DataType, targetType: DataType): DqRule = {
    if (Cast.canCast(sourceType, targetType)) {
      namedRule(fieldName, "type", s"$fieldName IS NULL OR CAST($fieldName AS ${targetType.sql}) IS NOT NULL")
    } else {
      val description = s"Field '$fieldName' type '$sourceType' has to be castable to type '$targetType'."
      namedRule(fieldName, "type", "false", description)
    }
  }

  def uniqueRule(fieldName: String): DqRule = {
    uniqueRule(fieldName, Seq(fieldName))
  }

  // composite and multi field rules

  def all(ruleName: String, rules: Seq[DqRule], description: String = null): DqRule = {
    rule(ruleName, rules.map(_.expression).mkString(" AND "), description)
  }

  def any(ruleName: String, rules: Seq[DqRule], description: String = null): DqRule = {
    rule(ruleName, rules.map(_.expression).mkString(" OR "), description)
  }

  def uniqueRule(baseRuleName: String, fieldNames: Seq[String]): DqRule = {
    namedRule(baseRuleName, "unique", s"(COUNT(*) OVER (PARTITION BY ${fieldNames.mkString(", ")})) = 1")
  }
}
