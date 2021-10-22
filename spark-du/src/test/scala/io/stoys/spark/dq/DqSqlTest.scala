package io.stoys.spark.dq

import io.stoys.spark.SToysException
import io.stoys.spark.test.SparkTestBase
import org.scalactic.source

class DqSqlTest extends SparkTestBase {
  import DqRules._
  import DqSql._

  test("parseDqSql - fails") {
    def im(dqSql: String, regex: String)(implicit pos: source.Position): SToysException = {
      interceptMessage[SToysException](parseDqSql(sparkSession, dqSql), regex)
    }

    sparkSession.sql("SELECT '1' AS k, 42.0 AS v, 'foo' AS e").createOrReplaceTempView("table")

    val dqSql = "SELECT *, k IS NOT NULL AS k__not_null FROM table"
    val expectedRule = namedRule("k", "not_null", s"(${quoteIfNeeded("k")} IS NOT NULL)")
    assert(parseDqSql(sparkSession, dqSql) === DqSqlInfo(Seq(expectedRule), Seq("table")))
    val noStarDqSql = "SELECT k IS NOT NULL AS k__not_null FROM table"
    im(noStarDqSql, "dq sql has to be '*'")
    val tableStarDqSql = "SELECT table.*, k IS NOT NULL AS k__not_null FROM table"
    assert(parseDqSql(sparkSession, tableStarDqSql) === DqSqlInfo(Seq(expectedRule), Seq("table")))
    val unnamedRuleDqSql = "SELECT *, k IS NOT NULL FROM table"
    im(unnamedRuleDqSql, "needs logical name")

    val ddlStatement = "DROP TABLE table"
    im(ddlStatement, "Unsupported logical plan")
  }

  test("parseDqSql - complex") {
    sparkSession.sql("SELECT '1' AS k, 42.0 AS v, 'foo' AS e").createOrReplaceTempView("table")
    sparkSession.sql("SELECT '1' AS id, 'desc' AS description").createOrReplaceTempView("lookup")

    val dqSql =
      s"""
         |WITH tmp AS (
         |  SELECT * FROM table JOIN lookup ON table.k = lookup.id
         |)
         |SELECT
         |  *,
         |  k IS NOT NULL AS k__not_null,
         |  k IS NOT NULL AND (k % 2 = 0) AS k__odd,
         |  -- optional
         |  -- comment
         |  e IN ('foo', 'bar', 'baz') AS e__enum_value
         |FROM tmp
         |""".stripMargin.trim

    val k = quoteIfNeeded("k")
    val e = quoteIfNeeded("e")
    val expectedRules = Seq(
      DqRule("k__not_null", s"($k IS NOT NULL)", None, Seq.empty),
      DqRule("k__odd", s"(($k IS NOT NULL) AND (($k % 2) = 0))", None, Seq.empty),
      DqRule("e__enum_value", s"($e IN ('foo', 'bar', 'baz'))", Some("optional\ncomment"), Seq.empty),
    )
    assert(parseDqSql(sparkSession, dqSql) === DqSqlInfo(expectedRules, Seq("lookup", "table")))
  }
}
