package com.nuna.trustdb.core

case class Metric(name: String, value: Double, labels: Map[String, String] = Map.empty)
