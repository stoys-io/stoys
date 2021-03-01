package io.stoys.spark.dq

import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.Row

import java.time.{Duration, Instant}

class DqTest extends SparkTestBase {
  import DqRules._
  import DqTest._
  import sparkSession.implicits._

  test("fromDqSql.computeDqResult") {
    records.toDS().createOrReplaceTempView(recordsTableName)

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

    val expectedDqResult = DqResult(
      columns = Seq(DqColumn("id"), DqColumn("value"), DqColumn("extra")),
      rules = Seq(
        DqRule("id__not_null", "(`id` IS NOT NULL)", None, Seq("id")),
        DqRule("id__odd", "((`id` IS NOT NULL) AND ((`id` % 2) = 0))", None, Seq("id")),
        DqRule("value__enum_value", "(`value` IN ('foo', 'bar', 'baz'))", None, Seq("value"))
      ),
      DqStatistics(
        table = DqTableStatistic(4, 3),
        column = Seq(DqColumnStatistics("id", 2), DqColumnStatistics("value", 2), DqColumnStatistics("extra", 0)),
        rule = Seq(
          DqRuleStatistics("id__not_null", 0),
          DqRuleStatistics("id__odd", 2),
          DqRuleStatistics("value__enum_value", 2)
        )
      ),
      row_sample = Seq(
        DqRowSample(Seq("1", "foo", "extra"), Seq("id__odd")),
        DqRowSample(Seq("3", "invalid", "extra"), Seq("id__odd", "value__enum_value")),
        DqRowSample(Seq("4", null, "extra"), Seq("value__enum_value"))
      ),
      Map.empty
    )

    val dqResult = Dq.fromDqSql(sparkSession, dqSql).computeDqResult().collect().head
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

    val dqResult = Dq.fromDqSql(sparkSession, dqSql).computeDqResult().collect().head
    assert(dqResult.statistics.table.violations === 2)
    assert(dqResult.statistics.rule.map(_.violations) === Seq(0, 2, 2))
  }

  test("fromFileInputPath.*") {
    val recordsCsvBasePath = writeTmpData("records.csv", records, "csv", Map("header" -> "true", "delimiter" -> "|"))
    val recordsCsvRelativePath = walkFileStatuses(recordsCsvBasePath.toString).keys.head
    val recordsCsvInputPath = s"$recordsCsvBasePath/$recordsCsvRelativePath?sos-format=csv&header=true&delimiter=%7C"

    val rules = Seq(namedRule("id", "even", "id IS NOT NULL AND (id % 2 = 0)"))
    // TODO: Can we fix double escaping in regexp?
    val fields = Seq(field("id", "integer", nullable = false, regexp = "\\\\d+"))

    val dq = Dq.fromFileInputPath(sparkSession, recordsCsvInputPath).rules(rules).fields(fields)

    val dqResult = dq.computeDqResult().collect().head
    assert(dqResult.statistics.table.violations === 2)
    assert(dqResult.metadata.get("size") === Some("66"))
    assert(Duration.between(Instant.parse(dqResult.metadata("modification_timestamp")), Instant.now()).getSeconds < 60)

    val dqViolationPerRowDs = dq.computeDqViolationPerRow()
    assert(dqViolationPerRowDs.collect() === Seq(
      DqViolationPerRow(Seq(), Seq("id"), Seq("1"), "id__even", "id IS NOT NULL AND (id % 2 = 0)"),
      DqViolationPerRow(Seq(), Seq("id"), Seq("3"), "id__even", "id IS NOT NULL AND (id % 2 = 0)")
    ))

    val failingRowsDs = dq.selectFailingRows()
    assert(failingRowsDs.collect() === Seq(Row("1", "foo", "extra"), Row("3", "invalid", "extra")))
  }

  test("fromDataset.*") {
    val rules = Seq(
      namedRule("id", "even", "id IS NOT NULL AND (id % 2 = 0)"),
      namedRule("id", "equal", "id = missing")
    )
    val fields = Seq(
      field("id", "integer", nullable = false),
      field("missing", "string")
    )
    val primaryKeyFieldNames = Seq("id", "missing_id")

    val dq = Dq.fromDataset(records.take(2).toDS())
        .rules(rules).fields(fields).primaryKeyFieldNames(primaryKeyFieldNames)

    val dqResult = dq.computeDqResult().collect().head
    assert(dqResult.statistics.table.violations === 2)

    val dqViolationPerRowDs = dq.computeDqViolationPerRow()
    assert(dqViolationPerRowDs.collect() === Seq(
      DqViolationPerRow(Seq("1", "__MISSING__"), Seq("id"), Seq("1"),
        "id__even", "id IS NOT NULL AND (id % 2 = 0)"),
      DqViolationPerRow(Seq("1", "__MISSING__"), Seq("id", "missing"), Seq("1", "__MISSING__"),
        "id__equal", "id = missing"),
      DqViolationPerRow(Seq("1", "__MISSING__"), Seq("missing", "missing_id"), Seq("__MISSING__", "__MISSING__"),
        "_expected_fields__exist", "false"),
      DqViolationPerRow(Seq("2", "__MISSING__"), Seq("id", "missing"), Seq("2", "__MISSING__"),
        "id__equal", "id = missing"),
      DqViolationPerRow(Seq("2", "__MISSING__"), Seq("missing", "missing_id"), Seq("__MISSING__", "__MISSING__"),
        "_expected_fields__exist", "false")
    ))

    val failingRowsDs = dq.selectFailingRows()
    assert(failingRowsDs.collect() === Seq(Row(1, "foo", "extra"), Row(2, "bar", "extra")))
  }
}

object DqTest {
  case class Record(id: Int, value: String, extra: String)

  val records: Seq[Record] = Seq(
    Record(1, "foo", "extra"),
    Record(2, "bar", "extra"),
    Record(3, "invalid", "extra"),
    Record(4, null, "extra")
  )

  val recordsTableName: String = "record"
}
