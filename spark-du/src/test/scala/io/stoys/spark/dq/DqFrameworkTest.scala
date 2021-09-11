package io.stoys.spark.dq

import io.stoys.spark.SToysException
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.scalactic.source

class DqFrameworkTest extends SparkTestBase {
  import DqFramework._
  import DqRules._

  test("getRuleInfo") {
    val ss = sparkSession
    val rule = notNullRule("Foo")
    assert(getRuleInfo(ss, Seq("foo"), Seq(rule)) === Seq(ruleInfo(rule, Seq("foo"), Seq(0), Seq.empty)))
    assert(getRuleInfo(ss, Seq("FOO"), Seq(rule)) === Seq(ruleInfo(rule, Seq("FOO"), Seq(0), Seq.empty)))
    assert(getRuleInfo(ss, Seq("bar"), Seq(rule)) === Seq(ruleInfo(rule, Seq.empty, Seq.empty, Seq("Foo"))))
    assert(getRuleInfo(ss, Seq("bar"), Seq(rule)) === Seq(ruleInfo(rule, Seq.empty, Seq.empty, Seq("Foo"))))
  }

  test("checkWideDqColumnsSanity") {
    def str(fieldName: String): StructField = StructField(fieldName, StringType)

    def bool(fieldName: String): StructField = StructField(fieldName, BooleanType)

    def schema(fields: StructField*): StructType = StructType(fields)

    def im(wideDqSchema: StructType, ruleCount: Int, regex: String)(implicit pos: source.Position): SToysException = {
      interceptMessage[SToysException](checkWideDqColumnsSanity(wideDqSchema, ruleCount), regex)
    }

    assert(checkWideDqColumnsSanity(schema(str("f1"), bool("r1")), 1))
    im(schema(str("f1"), str("r1")), 1, "return boolean values")
    im(schema(str("f1"), bool("r1"), bool("r1")), 2, "have unique names")
    im(schema(str("fr1"), bool("fr1")), 1, "have unique names")
    im(schema(str("FR1"), bool("fr1")), 1, "have unique names")
  }

  private def ruleInfo(rule: DqRule, existing: Seq[String], existingIndexes: Seq[Int],
      missing: Seq[String]): RuleInfo = {
    RuleInfo(rule, ColumnNamesInfo(existing ++ missing, existing, existingIndexes, missing))
  }
}
