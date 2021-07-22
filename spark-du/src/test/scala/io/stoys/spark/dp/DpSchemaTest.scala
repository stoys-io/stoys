package io.stoys.spark.dp

import io.stoys.scala.Arbitrary
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.types._

class DpSchemaTest extends SparkTestBase {
  import io.stoys.spark.test.implicits._

  private lazy val emptyDpColumn = Arbitrary.empty[DpColumn]

  test("DpSchema") {
    val sourceFields = Seq(
      StructField("i", StringType, nullable = true),
      StructField("e", StringType, nullable = false),
      StructField("dt", DateType),
      StructField("u", DateType)
    )
    val sourceSchema = StructType(sourceFields)
    val dpResult = DpResult(
      table = DpTable(42),
      columns = Seq(
        emptyDpColumn.copy(name = "i", data_type_json = "\"integer\"", nullable = false),
        emptyDpColumn.copy(
          name = "e", data_type_json = "\"string\"", nullable = true, enum_values = Seq("foo", "bar", "baz")),
        emptyDpColumn.copy(name = "dt", data_type_json = "\"date\"", nullable = false, format = "yyyy 🐧 MM?dd")
      )
    )
    val fooBarBazMetadata = Metadata.fromJson("""{"enum_values": ["foo", "bar", "baz"]}""")
    val dateMetadata = Metadata.fromJson("""{"format": "yyyy 🐧 MM?dd"}""")
    val expectedFields = Seq(
      StructField("i", IntegerType, nullable = false),
      StructField("e", StringType, nullable = true, metadata = fooBarBazMetadata),
      StructField("dt", DateType, nullable = false, metadata = dateMetadata),
      StructField("u", DateType)
    )

    assert(DpSchema.toSchema(dpResult).json === StructType(expectedFields.filterNot(_.name.startsWith("u"))).json)
    assert(DpSchema.updateSchema(sourceSchema, dpResult).json === StructType(expectedFields).json)
  }
}
