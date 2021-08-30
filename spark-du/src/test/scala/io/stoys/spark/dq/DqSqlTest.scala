package io.stoys.spark.dq

import io.stoys.spark.SToysException
import io.stoys.spark.test.SparkTestBase

class DqSqlTest extends SparkTestBase {
  import DqRules._
  import DqSql._

  test("parseReferencedColumnNames") {
    assert(parseReferencedColumnNames(sparkSession, "foo IS NOT NULL") === Seq("foo"))
    assert(parseReferencedColumnNames(sparkSession, "foo = bar") === Seq("foo", "bar"))
    assert(parseReferencedColumnNames(sparkSession, "foo IN ('bar') AND foo RLIKE '.*baz.*'") === Seq("foo"))
    assert(parseReferencedColumnNames(sparkSession, "table.foo IS NOT NULL") === Seq("table.foo"))
  }

  test("parseDqSql - exceptions") {
    def im(dqSql: String): String = {
      intercept[SToysException](parseDqSql(sparkSession, dqSql)).getMessage
    }

    val dqSql = "SELECT *, id IS NOT NULL AS id__not_null FROM table"
    val rules = Seq(namedRule("id", "not_null", "(`id` IS NOT NULL)"))
    val parsedDqSql = ParsedDqSql(rules, Set.empty)

    assert(parseDqSql(sparkSession, dqSql) === parsedDqSql)

    val noStarDqSql = "SELECT id IS NOT NULL AS id__not_null FROM table"
    assert(im(noStarDqSql).contains("dq sql has to be '*'"))
    val tableStarDqSql = "SELECT table.*, id IS NOT NULL AS id__not_null FROM table"
    assert(parseDqSql(sparkSession, tableStarDqSql) === parsedDqSql)
    val unnamedRuleDqSql = "SELECT *, id IS NOT NULL FROM table"
    assert(im(unnamedRuleDqSql).contains("needs logical name"))

    val unsupportedLogicalPlanSql = "DROP TABLE table"
    assert(im(unsupportedLogicalPlanSql).contains("Unsupported logical plan"))
  }

  test("parseDqSql - complex") {
    val dqSql =
      s"""
         |WITH table AS (
         |  SELECT * FROM table_a AS a JOIN table_b ON a.id = table_b.id
         |)
         |SELECT
         |  *,
         |  id IS NOT NULL AS id__not_null,
         |  id IS NOT NULL AND (id % 2 = 0) AS id__odd,
         |  -- optional
         |  -- comment
         |  value IN ('foo', 'bar', 'baz') AS value__enum_value
         |FROM
         |  table
         |""".stripMargin.trim

    val actual = parseDqSql(sparkSession, dqSql)
    val expectedRules = Seq(
      DqRule("id__not_null", "(`id` IS NOT NULL)", None, Seq.empty),
      DqRule("id__odd", "((`id` IS NOT NULL) AND ((`id` % 2) = 0))", None, Seq.empty),
      DqRule("value__enum_value", "(`value` IN ('foo', 'bar', 'baz'))", Some("optional\ncomment"), Seq.empty)
    )
    assert(actual.referencedTableNames === Set.empty)
    assert(actual.rules === expectedRules)
  }
}
