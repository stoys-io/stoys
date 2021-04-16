package io.stoys.spark.dp

import io.stoys.spark.MetadataKeys
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}

object DpSchema {
  private def toStructField(column: DpColumn): StructField = {
    val dataType = DataType.fromJson(s""""${column.data_type}"""")
    val metadataBuilder = new MetadataBuilder()
    if (column.enum_values != null && column.enum_values.nonEmpty) {
      metadataBuilder.putStringArray(MetadataKeys.ENUM_VALUES_KEY, column.enum_values.toArray)
    }
    column.format.foreach(f => metadataBuilder.putString(MetadataKeys.FORMAT_KEY, f))
    StructField(column.name, dataType, column.nullable, metadataBuilder.build())
  }

  def toSchema(result: DpResult): StructType = {
    StructType(result.columns.map(apc => toStructField(apc)))
  }

  def updateSchema(schema: StructType, result: DpResult): StructType = {
    val dpColumnByColumnName = result.columns.map(c => c.name -> c).toMap
    val fields = schema.fields.map { field =>
      dpColumnByColumnName.get(field.name) match {
        case Some(column) if column.data_type != null => toStructField(column)
        case _ => field
      }
    }
    StructType(fields)
  }
}
