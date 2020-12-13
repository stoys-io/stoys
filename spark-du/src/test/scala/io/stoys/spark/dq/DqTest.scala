package io.stoys.spark.dq

import java.time.{Duration, Instant}

import io.stoys.spark.SToysException
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

class DqTest extends SparkTestBase {
  import DqRules._
  import DqTest._
  import sparkSession.implicits._

  lazy val dq = new Dq(sparkSession)

  test("getRuleInfo") {
    val rule = notNullRule("Foo")
    assert(dq.getRuleInfo(Seq("foo"), Seq(rule)) === Seq(Dq.RuleInfo(rule, Seq.empty, Seq("foo"), Seq(0))))
    assert(dq.getRuleInfo(Seq("FOO"), Seq(rule)) === Seq(Dq.RuleInfo(rule, Seq.empty, Seq("FOO"), Seq(0))))
    assert(dq.getRuleInfo(Seq("bar"), Seq(rule)) === Seq(Dq.RuleInfo(rule, Seq("Foo"), Seq.empty, Seq.empty)))
    assert(dq.getRuleInfo(Seq("bar"), Seq(rule)) === Seq(Dq.RuleInfo(rule, Seq("Foo"), Seq.empty, Seq.empty)))
  }

  test("checkWideDqColumnsSanity") {
    def str(fieldName: String): StructField = StructField(fieldName, StringType)
    def bool(fieldName: String): StructField = StructField(fieldName, BooleanType)
    def schema(fields: StructField*): StructType = StructType(fields)
    def im(wideDqSchema: StructType, ruleCount: Int): String = {
      intercept[SToysException](dq.checkWideDqColumnsSanity(wideDqSchema, ruleCount)).getMessage
    }

    assert(dq.checkWideDqColumnsSanity(schema(str("f1"), bool("r1")), 1))
    assert(im(schema(str("f1"), str("r1")), 1).contains("return boolean values"))
    assert(im(schema(str("f1"), bool("r1"), bool("r1")), 2).contains("have unique names"))
    assert(im(schema(str("fr1"), bool("fr1")), 1).contains("have unique names"))
    assert(im(schema(str("FR1"), bool("fr1")), 1).contains("have unique names"))
  }

  test("dqSql") {
    records.toDS().createOrReplaceTempView(recordsTableName)
    val dqResult = dq.dqSql(recordsDqSql).collect().head
    assert(dqResult.statistics.rule.map(_.violations) === Seq(0, 2, 2))
  }

  test("dqSql - complex") {
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

    records.toDS().createOrReplaceTempView(recordsTableName)
    Seq("foo", "bar", "baz").toDF("value").createOrReplaceTempView("lookup")
    val dqResult = dq.dqSql(dqSql).collect().head
    assert(dqResult.statistics.table.violations === 2)
    assert(dqResult.statistics.rule.map(_.violations) === Seq(0, 2, 2))
  }

  test("dqFile*") {
    val recordsCsvBasePath = s"$tmpDir/dqFile/record.csv"
    records.toDS().write.format("csv").option("header", "true").option("delimiter", "|").save(recordsCsvBasePath)
    val recordsCsvRelativePath = walkDfsFileStatusesByRelativePath(recordsCsvBasePath).keys.head
    val recordsCsvInputPath = s"$recordsCsvBasePath/$recordsCsvRelativePath?sos-format=csv&header=true&delimiter=%7C"

    val rules = Seq(namedRule("id", "even", "id IS NOT NULL AND (id % 2 = 0)"))
    // TODO: Can we fix double escaping in regexp?
    val fields = Seq(field("id", "integer", nullable = false, regexp = "\\\\d+"))
    val primaryKeyFieldNames = Seq("id")

    val dqResult = dq.dqFile(recordsCsvInputPath, rules, fields, primaryKeyFieldNames).collect().head
    assert(dqResult.statistics.table.violations === 2)
    assert(dqResult.metadata.get("size") === Some("66"))
    assert(Duration.between(Instant.parse(dqResult.metadata("modification_timestamp")), Instant.now()).getSeconds < 60)

    val dqViolationPerRowDs = dq.dqFileViolationPerRow(recordsCsvInputPath, rules, fields, primaryKeyFieldNames)
    assert(dqViolationPerRowDs.collect() === Seq(
      DqViolationPerRow(Seq("1"), Seq("id"), Seq("1"), "id__even", "id IS NOT NULL AND (id % 2 = 0)"),
      DqViolationPerRow(Seq("3"), Seq("id"), Seq("3"), "id__even", "id IS NOT NULL AND (id % 2 = 0)")
    ))
  }

  // TODO: test non boolean rules, non unique rules, dqTable, dqDataset
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

  val recordsDqSql: String =
    s"""
       |SELECT
       |  *,
       |  id IS NOT NULL AS id__not_null,
       |  id IS NOT NULL AND (id % 2 = 0) AS id__odd,
       |  value IN ('foo', 'bar', 'baz') AS value__enum_value
       |FROM
       |  $recordsTableName
       |""".stripMargin.trim
}
