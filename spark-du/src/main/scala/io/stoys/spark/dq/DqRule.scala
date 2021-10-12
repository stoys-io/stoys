package io.stoys.spark.dq

case class DqRule(
    name: String,
    expression: String,
    description: Option[String],
    referenced_column_names: Seq[String],
)
