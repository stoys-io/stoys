package io.stoys.spark.dq

case class DqField(
    name: String,
    typ: String,
    nullable: Boolean,
    enum_values: Seq[String],
    format: Option[String],
    regexp: Option[String]
)
