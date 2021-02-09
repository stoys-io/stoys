package io.stoys.spark.dq

import java.sql.Date

import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

class DqSchemaTest extends SparkTestBase {
  import DqRules._
  import DqSchema._
  import DqSchemaTest._

  test("generateSchemaRules") {
    val existingSchema = ScalaReflection.schemaFor[Record].dataType.asInstanceOf[StructType]
    val expectedFields = Seq(
      field("i", "integer"),
      field("s", "string", nullable = false, enumValues = Seq("foo", "bar", "baz"), regexp = "(foo|bar|baz)"),
      field("d", "date", format = "MM/dd/yyyy"),
      field("missing", "string")
    )
    val primaryKeyFieldNames = Seq("i", "s")

    val expectedRules = Seq(
      DqRule("_expected_fields__exist", "false", Some("Expected fields should exist: missing"), Seq("missing")),
      DqRule("_primary_key__not_null", "i IS NOT NULL AND s IS NOT NULL", None, Seq.empty),
      DqRule("_primary_key__unique", "(COUNT(*) OVER (PARTITION BY i, s)) = 1", None, Seq.empty),
      DqRule("i__type", "i IS NULL OR CAST(i AS INT) IS NOT NULL", None, Seq.empty),
      DqRule("s__type", "s IS NULL OR CAST(s AS STRING) IS NOT NULL", None, Seq.empty),
      DqRule("s__not_null", "s IS NOT NULL", None, Seq.empty),
      DqRule("s__enum_values", "CAST(s AS STRING) IN ('foo', 'bar', 'baz')", None, Seq.empty),
      DqRule("s__regexp", "CAST(s AS STRING) RLIKE '(foo|bar|baz)'", None, Seq.empty),
      DqRule("d__type", "d IS NULL OR TO_DATE(d, 'MM/dd/yyyy') IS NOT NULL", None, Seq.empty)
    )
    val rules = generateSchemaRules(existingSchema, expectedFields, primaryKeyFieldNames, DqConfig.default)
    assert(rules === expectedRules)

    val extraColumnConfig = DqConfig.default.copy(report_extra_columns = true)
    val extraColumnRules = generateSchemaRules(existingSchema, expectedFields, primaryKeyFieldNames, extraColumnConfig)
    val expectedExtraColumnRule =
      DqRule("_extra_fields__not_exist", "false", Some("Extra fields should not exist: b, f, a, extra"), Seq.empty)
    assert(extraColumnRules.find(_.name.contains("extra")) === Some(expectedExtraColumnRule))
  }
}

object DqSchemaTest {
  case class Record(b: Boolean, i: Int, f: Float, s: String, a: Array[String], d: Date, extra: String)
}
