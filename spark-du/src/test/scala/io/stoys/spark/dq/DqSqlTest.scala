package io.stoys.spark.dq

import io.stoys.spark.SToysException
import io.stoys.spark.test.SparkTestBase
import org.scalactic.source

class DqSqlTest extends SparkTestBase {
  import DqRules._
  import DqSql._

  test("parseReferencedColumnNames") {
    assert(parseReferencedColumnNames(sparkSession, "foo IS NOT NULL") === Seq("foo"))
    assert(parseReferencedColumnNames(sparkSession, "foo = bar") === Seq("foo", "bar"))
    assert(parseReferencedColumnNames(sparkSession, "foo IN ('bar') AND foo RLIKE '.*baz.*'") === Seq("foo"))
    assert(parseReferencedColumnNames(sparkSession, "table.foo IS NOT NULL") === Seq("table.foo"))
  }

  test("parseDqSql - fails") {
    def im(dqSql: String, regex: String)(implicit pos: source.Position): SToysException = {
      interceptMessage[SToysException](parseDqSql(sparkSession, dqSql), regex)
    }

    val dqSql = "SELECT *, id IS NOT NULL AS id__not_null FROM table"
    val expectedRule = namedRule("id", "not_null", s"(${quoteIfNeeded("id")} IS NOT NULL)")
    assert(parseDqSql(sparkSession, dqSql) === ParsedDqSql(Seq(expectedRule), Set.empty))
    val noStarDqSql = "SELECT id IS NOT NULL AS id__not_null FROM table"
    im(noStarDqSql, "dq sql has to be '*'")
    val tableStarDqSql = "SELECT table.*, id IS NOT NULL AS id__not_null FROM table"
    assert(parseDqSql(sparkSession, tableStarDqSql) === ParsedDqSql(Seq(expectedRule), Set.empty))
    val unnamedRuleDqSql = "SELECT *, id IS NOT NULL FROM table"
    im(unnamedRuleDqSql, "needs logical name")

    val ddlStatement = "DROP TABLE table"
    im(ddlStatement, "Unsupported logical plan")
    val dmlStatement = "INSERT INTO table VALUES ('foo')"
    im(dmlStatement, "Unsupported logical plan")
    val auxiliaryStatement = "SHOW TABLES"
    im(auxiliaryStatement, "Unsupported logical plan")
    val explainStatement = "EXPLAIN SELECT 'foo'"
    im(explainStatement, "Unsupported logical plan")
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

    val id = quoteIfNeeded("id")
    val value = quoteIfNeeded("value")
    val expectedRules = Seq(
      DqRule("id__not_null", s"($id IS NOT NULL)", None, Seq.empty),
      DqRule("id__odd", s"(($id IS NOT NULL) AND (($id % 2) = 0))", None, Seq.empty),
      DqRule("value__enum_value", s"($value IN ('foo', 'bar', 'baz'))", Some("optional\ncomment"), Seq.empty),
    )
    assert(parseDqSql(sparkSession, dqSql) === ParsedDqSql(expectedRules, Set.empty))
  }
}
