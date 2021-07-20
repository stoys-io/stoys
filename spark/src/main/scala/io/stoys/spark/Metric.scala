package io.stoys.spark

case class Metric(
    key: String,
    value: Double,
    labels: Map[String, String] = Map.empty
)
