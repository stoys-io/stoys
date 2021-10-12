package io.stoys.spark.dq

case class DqField(
    name: String,
    data_type_json: String,
    nullable: Boolean,
    enum_values: Seq[String],
    format: Option[String],
    regexp: Option[String],
)
