package io.stoys.spark.dq

case class DqViolationPerRow(
    primary_key: Seq[String],
    column_names: Seq[String],
    values: Seq[String],
    rule_name: String,
    rule_expression: String,
)
