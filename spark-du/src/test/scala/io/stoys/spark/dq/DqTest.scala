package io.stoys.spark.dq

import io.stoys.spark.Dfs
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.Row

import java.time.{Duration, Instant}

class DqTest extends SparkTestBase {
  import DqRules._
  import DqTest._
  import sparkSession.implicits._

  private val records = Seq(
    Record(1, "foo", "extra"),
    Record(2, "bar", "extra"),
    Record(3, "invalid", "extra"),
    Record(4, null, "extra"),
  )

  private val recordsTableName = "record"

  test("fromDataset.*") {
    val rules = Seq(
      namedRule("id", "even", "id IS NOT NULL AND (id % 2 = 0)"),
      namedRule("id", "equal", "id = missing"),
    )
    val fields = Seq(
      field("id", "\"integer\"", nullable = false),
      field("missing", "\"string\""),
    )
    val primaryKeyFieldNames = Seq("id", "missing_id")

    val dq = Dq.fromDataset(records.take(2).toDS())
        .rules(rules).fields(fields).primaryKeyFieldNames(primaryKeyFieldNames)

    val dqResult = dq.computeDqResult().first()
    assert(dqResult.statistics.table.violations === 2)

    val dqViolationPerRowDs = dq.computeDqViolationPerRow()
    assert(dqViolationPerRowDs.collect() === Seq(
      DqViolationPerRow(Seq("1", "__MISSING__"), Seq("id"), Seq("1"),
        "id__even", "id IS NOT NULL AND (id % 2 = 0)"),
      DqViolationPerRow(Seq("1", "__MISSING__"), Seq("id", "missing"), Seq("1", "__MISSING__"),
        "id__equal", "id = missing"),
      DqViolationPerRow(Seq("1", "__MISSING__"), Seq("missing", "missing_id"), Seq("__MISSING__", "__MISSING__"),
        "__no_missing_fields", "false"),
      DqViolationPerRow(Seq("2", "__MISSING__"), Seq("id", "missing"), Seq("2", "__MISSING__"),
        "id__equal", "id = missing"),
      DqViolationPerRow(Seq("2", "__MISSING__"), Seq("missing", "missing_id"), Seq("__MISSING__", "__MISSING__"),
        "__no_missing_fields", "false"),
    ))

    val failingRowsDs = dq.selectFailingRows()
    assert(failingRowsDs.collect() === Seq(Row(1, "foo", "extra"), Row(2, "bar", "extra")))

    val passingRowsDs = dq.selectPassingRows()
    assert(passingRowsDs.collect() === Seq.empty)
  }

  test("fromDqSql.computeDqResult") {
    val recordsDs = records.toDS()
    recordsDs.createOrReplaceTempView(recordsTableName)

    val dqSql: String =
      s"""
         |SELECT
         |  *,
         |  id IS NOT NULL AS id__not_null,
         |  id IS NOT NULL AND (id % 2 = 0) AS id__odd,
         |  value IN ('foo', 'bar', 'baz') AS value__enum_value
         |FROM
         |  $recordsTableName
         |""".stripMargin.trim

    val id = quoteIfNeeded("id")
    val value = quoteIfNeeded("value")
    val expectedDqResult = DqResult(
      columns = Seq(DqColumn("id"), DqColumn("value"), DqColumn("extra")),
      rules = Seq(
        DqRule("id__not_null", s"($id IS NOT NULL)", None, Seq("id")),
        DqRule("id__odd", s"(($id IS NOT NULL) AND (($id % 2) = 0))", None, Seq("id")),
        DqRule("value__enum_value", s"($value IN ('foo', 'bar', 'baz'))", None, Seq("value")),
      ),
      statistics = DqStatistics(
        table = DqTableStatistic(4, 3),
        column = Seq(DqColumnStatistics("id", 2), DqColumnStatistics("value", 2), DqColumnStatistics("extra", 0)),
        rule = Seq(
          DqRuleStatistics("id__not_null", 0),
          DqRuleStatistics("id__odd", 2),
          DqRuleStatistics("value__enum_value", 2),
        ),
      ),
      row_sample = Seq(
        DqRowSample(Seq("1", "foo", "extra"), Seq("id__odd")),
        DqRowSample(Seq("3", "invalid", "extra"), Seq("id__odd", "value__enum_value")),
        DqRowSample(Seq("4", null, "extra"), Seq("value__enum_value")),
      ),
      metadata = Map(
        "dq_sql" -> dqSql,
        "data_type_json" -> recordsDs.schema.json,
      ),
    )

    val dqResult = Dq.fromDqSql(sparkSession, dqSql).computeDqResult().first()
    assert(dqResult === expectedDqResult)
  }

  test("fromDqSql.computeDqResult - complex") {
    records.toDS().createOrReplaceTempView(recordsTableName)
    Seq("foo", "bar", "baz").toDF("value").createOrReplaceTempView("lookup")

    val dqSql =
      s"""
         |WITH ${recordsTableName}_plus AS (
         |  SELECT *, 'plus' AS plus FROM $recordsTableName
         |)
         |SELECT
         |  t.*,
         |  t.id IS NOT NULL AS id__not_null,
         |--  (COUNT(*) OVER (PARTITION BY t.id)) = 1 AS id__unique,
         |  LENGTH(t.value) < LENGTH(extra) AS value__shorter_then_extra,
         |--  lookup.value IS NOT NULL AS value__in_lookup_join,
         |  -- this should be: EXISTS (SELECT * FROM lookup AS l WHERE t.value = l.value) AS value__in_lookup_exists
         |  (SELECT FIRST(l.value) FROM lookup AS l WHERE t.value = l.value) IS NOT NULL AS value__in_lookup_sub_select
         |--FROM ${recordsTableName}_plus AS t LEFT JOIN lookup ON t.value = lookup.value
         |FROM $recordsTableName AS t
         |ORDER BY t.extra
         |LIMIT 42
         |""".stripMargin.trim

    val dqResult = Dq.fromDqSql(sparkSession, dqSql).computeDqResult().first()
    assert(dqResult.statistics.table.violations === 2)
    assert(dqResult.statistics.rule.map(_.violations) === Seq(0, 2, 2))
  }

  test("fromFileInputPath.computeDqResult") {
    val dfs = Dfs(sparkSession)
    val filePath = tmpDir.resolve("complex_records.csv")
    val fileInputPath = s"$filePath?sos-format=csv&header=true&delimiter=%3B"
    val fileContent =
      """
        |fOO +;Foo -;extra
        |foo;bar;baz
        |;bar;baz
        |foo;meh;baz
        |foo;bar;
        |foo;42;baz
        |invalid
        |""".stripMargin
    dfs.writeString(filePath.toString, fileContent)

    val fields = DqReflection.getDqFields[ComplexRecord]
    val rules = Seq(namedRule("foo+-", "same_length", "LENGTH(`foo +`) = LENGTH(`foo -`)"))
    val dq = Dq.fromFileInputPath(sparkSession, fileInputPath).fields(fields).rules(rules)

    val dqResult = dq.computeDqResult().first()
    assert(dqResult.columns.map(_.name) === Seq("fOO +", "Foo -", "extra", "__corrupt_record__", "__row_number__"))
    assert(dqResult.rules.map(_.name) === Seq("__record_not_corrupted", "foo+-__same_length",
      "fOO +__type", "fOO +__not_null", "fOO -__type", "fOO -__enum_values"))
    assert(dqResult.statistics.table === DqTableStatistic(rows = 6, violations = 4))
    assert(dqResult.statistics.column === Seq(
      DqColumnStatistics("fOO +", 3),
      DqColumnStatistics("Foo -", 4),
      DqColumnStatistics("extra", 0),
      DqColumnStatistics("__corrupt_record__", 1),
      DqColumnStatistics("__row_number__", 0),
    ))
    assert(dqResult.statistics.rule === Seq(
      DqRuleStatistics("__record_not_corrupted", 1),
      DqRuleStatistics("foo+-__same_length", 3),
      DqRuleStatistics("fOO +__type", 0),
      DqRuleStatistics("fOO +__not_null", 1),
      DqRuleStatistics("fOO -__type", 0),
      DqRuleStatistics("fOO -__enum_values", 2),
    ))
    assert(dqResult.row_sample === Seq(
      DqRowSample(Seq(null, "bar", "baz", null, "1"), Seq("foo+-__same_length", "fOO +__not_null")),
      DqRowSample(Seq("foo", "meh", "baz", null, "2"), Seq("fOO -__enum_values")),
      DqRowSample(Seq("foo", "42", "baz", null, "4"), Seq("foo+-__same_length", "fOO -__enum_values")),
      DqRowSample(Seq("invalid", null, null, "invalid", "5"), Seq("__record_not_corrupted", "foo+-__same_length")),
    ))
    assert(dqResult.metadata.keySet
        === Set("input_path", "path", "file_name", "size", "modification_timestamp", "table_name", "data_type_json"))
    assert(dqResult.metadata.get("table_name") === Some("complex_records_csv"))
    assert(dqResult.metadata.get("size") === Some("80"))
    assert(Duration.between(Instant.parse(dqResult.metadata("modification_timestamp")), Instant.now()).getSeconds < 60)
  }
}

object DqTest {
  import annotation.DqField

  case class Record(
      id: Int,
      value: String,
      extra: String,
  )

  case class ComplexRecord(
      @DqField(nullable = false)
      `fOO +`: String,
      @DqField(enumValues = Array("foo", "bar", "baz"))
      `fOO -`: String,
  )
}
