package com.nuna.trustdb.core

case class Metric(key: String, value: Double, labels: Map[String, String] = Map.empty)
