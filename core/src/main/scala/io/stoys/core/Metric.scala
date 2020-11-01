package io.stoys.core

case class Metric(key: String, value: Double, labels: Map[String, String] = Map.empty)
