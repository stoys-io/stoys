package io.stoys.spark.dq

import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

class DqSchemaTest extends SparkTestBase {
  import DqRules._
  import DqSchema._
  import DqSchemaTest._

  test("generateSchemaRules - Record") {
    val existingSchema = ScalaReflection.schemaFor[Record].dataType.asInstanceOf[StructType]
    val expectedFields = Seq(
      field("b", "\"null\""),
      field("i", "\"integer\""),
      field("a", """{"type":"array","elementType":"integer","containsNull":true}"""),
      field("s", "\"string\"", nullable = false, enumValues = Seq("foo", "bar", "baz"), regexp = "(foo|bar|baz)"),
      field("d", "\"date\"", format = "MM/dd/yyyy"),
      field("missing", "\"string\"")
    )
    val primaryKeyFieldNames = Seq("i", "s")

    val expectedRules = Seq(
      DqRule("_expected_fields__exist", "false", Some("Expected fields should exist: missing"), Seq("missing")),
      DqRule("_primary_key__not_null", "`i` IS NOT NULL AND `s` IS NOT NULL", None, Seq.empty),
      DqRule("_primary_key__unique", "(COUNT(*) OVER (PARTITION BY `i`, `s`)) = 1", None, Seq.empty),
      DqRule("b__type", "false", Some("Cannot cast `b` from 'StringType' to 'NullType'."), Seq.empty),
      DqRule("i__type", "`i` IS NULL OR (CAST(`i` AS INT) IS NOT NULL)", None, Seq.empty),
      DqRule("a__type", "`a` IS NULL OR (CAST(`a` AS ARRAY<INT>) IS NOT NULL)", None, Seq.empty),
      DqRule("s__type", "`s` IS NULL OR (CAST(`s` AS STRING) IS NOT NULL)", None, Seq.empty),
      DqRule("s__not_null", "`s` IS NOT NULL", None, Seq.empty),
      DqRule("s__enum_values", "`s` IS NULL OR (CAST(`s` AS STRING) IN ('foo', 'bar', 'baz'))", None, Seq.empty),
      DqRule("s__regexp", "`s` IS NULL OR (CAST(`s` AS STRING) RLIKE '(foo|bar|baz)')", None, Seq.empty),
      DqRule("d__type", "`d` IS NULL OR (TO_DATE(`d`, 'MM/dd/yyyy') IS NOT NULL)", None, Seq.empty)
    )
    val rules = generateSchemaRules(existingSchema, expectedFields, primaryKeyFieldNames, DqConfig.default)
    assert(rules === expectedRules)
  }

  test("generateSchemaRules - PoorlyNamedRecord") {
    val existingSchema = ScalaReflection.schemaFor[PoorlyNamedRecord].dataType.asInstanceOf[StructType]
    val expectedFields = Seq(
      field("space s", "\"integer\""),
      field("symbols  (*)_42", "\"integer\""),
      field("mixed_CASE", "\"integer\"")
    )
    val primaryKeyFieldNames = Seq("space s", "symbols  (*)_42")

    val expectedRules = Seq(
      rule("_primary_key__not_null", "`space s` IS NOT NULL AND `symbols  (*)_42` IS NOT NULL"),
      rule("_primary_key__unique", "(COUNT(*) OVER (PARTITION BY `space s`, `symbols  (*)_42`)) = 1"),
      rule("space s__type", "`space s` IS NULL OR (CAST(`space s` AS INT) IS NOT NULL)"),
      rule("symbols  (*)_42__type", "`symbols  (*)_42` IS NULL OR (CAST(`symbols  (*)_42` AS INT) IS NOT NULL)"),
      rule("mixed_CASE__type", "`mixed_CASE` IS NULL OR (CAST(`mixed_CASE` AS INT) IS NOT NULL)")
    )
    val rules = generateSchemaRules(existingSchema, expectedFields, primaryKeyFieldNames, DqConfig.default)
    assert(rules === expectedRules)
  }

  test("generateSchemaRules - report_extra_columns") {
    val existingSchema = ScalaReflection.schemaFor[Record].dataType.asInstanceOf[StructType]

    assert(generateSchemaRules(existingSchema, Seq.empty, Seq.empty, DqConfig.default) === Seq.empty)

    val config = DqConfig.default.copy(report_extra_columns = true)
    val rules = generateSchemaRules(existingSchema, Seq.empty, Seq.empty, config)
    val expectedRules = Seq(
      rule("_extra_fields__not_exist", "false", "Extra fields should not exist: b, i, s, a, d, extra")
    )
    assert(rules === expectedRules)
  }
}

object DqSchemaTest {
  case class Record(b: String, i: String, s: String, a: Array[String], d: String, extra: String)
  case class PoorlyNamedRecord(`space s`: String, `symbols  (*)_42`: String, mixed_CASE: String)
}
