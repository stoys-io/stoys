package io.stoys.spark.aggsum

import io.stoys.spark.SToysException
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.AnalysisException

class AggSumTest extends SparkTestBase {
  test("fromSql.getAggSumInfo - fails") {
    def parseASI(sql: String): AggSumInfo = {
      AggSum.fromSql(sparkSession, sql).getAggSumInfo
    }

    sparkSession.sql("SELECT '1' AS k, 42.0 AS v, 'foo' AS e").createOrReplaceTempView("table")

    interceptMessage[AnalysisException](parseASI("SELECT k, SUM(v) FROM table"), "not an aggregate function")
    interceptMessage[SToysException](parseASI("SELECT k, v FROM table"), "Aggregate statement required")
    interceptMessage[SToysException](
      parseASI("SELECT k, FIRST(e) FROM table GROUP BY k"), "Aggregates have to be numerical")

    assert(parseASI("SELECT k, SUM(v) FROM table GROUP BY k")
        === AggSumInfo(Seq("k"), Seq("sum(v)"), Seq("table")))
    assert(parseASI("SELECT k, SUM(v) AS va FROM table GROUP BY k")
        === AggSumInfo(Seq("k"), Seq("va"), Seq("table")))
  }

  test("fromSql.computeAggSumResult - complex") {
    sparkSession.sql("SELECT '1' AS k, 42.0 AS v, 'foo' AS e").createOrReplaceTempView("table")
    sparkSession.sql("SELECT '1' AS id, 'desc' AS description").createOrReplaceTempView("lookup")

    val aggSumSql =
      """
        |WITH tmp AS (
        |  SELECT * FROM table CROSS JOIN lookup ON HASH(table.k, lookup.id) != 0
        |)
        |SELECT
        |  tmp.k,
        |  second_lookup.id,
        |  SUM(tmp.v) AS va,
        |  AVG(LENGTH(second_lookup.description) * CAST(tmp.k AS INT)) AS nonsense
        |FROM tmp
        |JOIN lookup AS second_lookup ON tmp.id = second_lookup.id
        |GROUP BY 1, 2
        |""".stripMargin.trim

    assert(AggSum.fromSql(sparkSession, aggSumSql).computeAggSumResult().first() === AggSumResult(
      aggsum_info = AggSumInfo(Seq("k", "id"), Seq("va", "nonsense"), Seq("lookup", "table")),
      data_json = """[{"k":"1","id":"1","va":42.0,"nonsense":4.0}]""",
    ))
  }
}
