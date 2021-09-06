package io.stoys.spark

import org.apache.spark.sql.types.StructField

object MetadataKeys {
  val ENUM_VALUES_KEY = "enum_values"
  val FORMAT_KEY = "format"

  def getEnumValues(field: StructField): Option[Array[String]] = {
    if (field.metadata.contains(ENUM_VALUES_KEY)) {
      Option(field.metadata.getStringArray(ENUM_VALUES_KEY))
    } else {
      None
    }
  }

  def getFormat(field: StructField): Option[String] = {
    if (field.metadata.contains(FORMAT_KEY)) {
      Option(field.metadata.getString(FORMAT_KEY))
    } else {
      None
    }
  }
}
