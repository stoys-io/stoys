package io.stoys.spark.dq

import io.stoys.spark.SToysException
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

class DqFrameworkTest extends SparkTestBase {
  import DqFramework._
  import DqRules._

  test("getRuleInfo") {
    val ss = sparkSession
    val rule = notNullRule("Foo")
    assert(getRuleInfo(ss, Seq("foo"), Seq(rule)) === Seq(RuleInfo(rule, Seq.empty, Seq("foo"), Seq(0))))
    assert(getRuleInfo(ss, Seq("FOO"), Seq(rule)) === Seq(RuleInfo(rule, Seq.empty, Seq("FOO"), Seq(0))))
    assert(getRuleInfo(ss, Seq("bar"), Seq(rule)) === Seq(RuleInfo(rule, Seq("Foo"), Seq.empty, Seq.empty)))
    assert(getRuleInfo(ss, Seq("bar"), Seq(rule)) === Seq(RuleInfo(rule, Seq("Foo"), Seq.empty, Seq.empty)))
  }

  test("checkWideDqColumnsSanity") {
    def str(fieldName: String): StructField = StructField(fieldName, StringType)

    def bool(fieldName: String): StructField = StructField(fieldName, BooleanType)

    def schema(fields: StructField*): StructType = StructType(fields)

    def im(wideDqSchema: StructType, ruleCount: Int): String = {
      intercept[SToysException](checkWideDqColumnsSanity(wideDqSchema, ruleCount)).getMessage
    }

    assert(checkWideDqColumnsSanity(schema(str("f1"), bool("r1")), 1))
    assert(im(schema(str("f1"), str("r1")), 1).contains("return boolean values"))
    assert(im(schema(str("f1"), bool("r1"), bool("r1")), 2).contains("have unique names"))
    assert(im(schema(str("fr1"), bool("fr1")), 1).contains("have unique names"))
    assert(im(schema(str("FR1"), bool("fr1")), 1).contains("have unique names"))
  }
}
