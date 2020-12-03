package io.stoys.spark.dq

case class DqField(
    name: String,
    typ: String,
    nullable: Boolean,
    regex: Option[String]
)
