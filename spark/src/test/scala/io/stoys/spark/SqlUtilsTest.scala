package io.stoys.spark

import io.stoys.spark.test.SparkTestBase
import org.scalactic.source

class SqlUtilsTest extends SparkTestBase {
  import SqlUtils._

  test("quoteIfNeeded") {
    assert(SqlUtils.quoteIfNeeded("foo") === "foo")
    assert(SqlUtils.quoteIfNeeded("f o o") === "`f o o`")
    assert(SqlUtils.quoteIfNeeded(" foo ") === "` foo `")
    assert(SqlUtils.quoteIfNeeded("f@o") === "`f@o`")
  }

  test("getReferencedColumnNames") {
    def getRCN(expression: String): Seq[String] = {
      getReferencedColumnNames(sparkSession.sessionState.sqlParser.parseExpression(expression))
    }

    assert(getRCN("foo IS NOT NULL") === Seq("foo"))
    assert(getRCN("foo = bar") === Seq("bar", "foo"))
    assert(getRCN("foo IN ('bar') AND foo RLIKE '.*baz.*'") === Seq("foo"))
    assert(getRCN("table.foo IS NOT NULL") === Seq("table.foo"))
    assert(getRCN("`f@o` = 42") === Seq("f@o"))
  }

  test("getReferencedRelationshipNames") {
    def getRRN(sql: String): Seq[String] = {
      getReferencedRelationshipNames(sparkSession.sessionState.sqlParser.parsePlan(sql))
    }

    assert(getRRN("SELECT * FROM foo JOIN bar ON foo.id = bar.id") === Seq("bar", "foo"))
    assert(getRRN("SELECT * FROM foo AS f") === Seq("foo"))
    assert(getRRN("SELECT f.* FROM foo AS f") === Seq("foo"))
    assert(getRRN("SELECT * FROM (SELECT * FROM foo) as bar") === Seq("foo"))
    assert(getRRN("WITH baz AS (SELECT * FROM foo) SELECT * FROM baz") === Seq("foo"))
    assert(getRRN("WITH baz AS (SELECT * FROM bar) SELECT * FROM foo") === Seq("foo"))
    assert(getRRN("WITH foo AS (SELECT * FROM foo) SELECT * FROM foo") === Seq("foo"))
  }

  test("assertQueryLogicalPlan") {
    def assertQLP(sql: String): Unit = {
      assertQueryLogicalPlan(sparkSession.sessionState.sqlParser.parsePlan(sql))
    }

    def im(sql: String, regex: String)(implicit pos: source.Position): SToysException = {
      interceptMessage[SToysException](assertQLP(sql), regex)
    }

    val selectStatement = "SELECT * FROM table"
    assertQLP(selectStatement)
    val aggregateStatement = "SELECT * FROM table GROUP BY c"
    assertQLP(aggregateStatement)
    val complexStatement = "WITH a AS (SELECT * FROM t) SELECT c FROM table GROUP BY c ORDER BY c LIMIT 10"
    assertQLP(complexStatement)

    val ddlStatement = "DROP TABLE table"
    im(ddlStatement, "Unsupported logical plan")
    val dmlStatement = "INSERT INTO table VALUES ('foo')"
    im(dmlStatement, "Unsupported logical plan")
    val auxiliaryStatement = "SHOW TABLES"
    im(auxiliaryStatement, "Unsupported logical plan")
    val explainStatement = "EXPLAIN SELECT 'foo'"
    im(explainStatement, "Unsupported logical plan")
  }
}
