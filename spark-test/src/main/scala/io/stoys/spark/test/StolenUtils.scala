package io.stoys.spark.test

// Copy a few utils from other modules to avoid circular dependencies.
private[test] object StolenUtils {
  // from io.stoys.scala.Strings
  def toWordCharactersCollapsing(value: String): String = {
    value.replaceAll("\\W[_\\W]*", "_")
  }
}
