package io.stoys.spark.dq

import io.stoys.spark.SToysException
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, IntegerType, StringType}

import scala.reflect.runtime.universe._

class DqRulesTest extends SparkTestBase {
  import DqRules._
  import sparkSession.implicits._

  test("name") {
    assert(name("id", "not_null") === "id__not_null")
  }

  test("namedRule") {
    val rule = namedRule("id", "even", "id IS NOT NULL AND (id % 2 = 0)")
    assert(eval(rule, "id", null) === false)
    assert(eval(rule, "id", 1) === false)
    assert(eval(rule, "id", 42) === true)
  }

  test("rule") {
    val expectedDqRule = DqRule("id__not_null", "id IS NOT NULL", Some("id should not be null"), Seq.empty)
    assert(rule("id__not_null", "id IS NOT NULL", "id should not be null") === expectedDqRule)

    import org.apache.spark.sql.functions._
    assertThrows[SToysException](rule(col("id").isNotNull))
    assert(rule(col("id").isNotNull.as("id__not_null"), "id should not be null")
        === expectedDqRule.copy(expression = "(`id` IS NOT NULL)"))
  }

  test("field") {
    assert(name("foo", "bar") === "foo__bar")
  }

  test("enumValuesRule") {
    val rule = enumValuesRule("sex", Seq("M", "F"))
    assert(eval(rule, "sex", null) === false)
    assert(eval(rule, "sex", "") === false)
    assert(eval(rule, "sex", "M") === true)
    assert(eval(rule, "sex", "F") === true)
    assert(eval(rule, "sex", "f") === false)
    assert(eval(rule, "sex", "female") === false)
    assert(eval(rule, "sex", 42) === false)

    val caseInsensitiveRule = enumValuesRule("sex", Seq("M", "F"), caseInsensitive = true)
    assert(eval(caseInsensitiveRule, "sex", "f") === true)
  }

  test("notNullRule") {
    val rule = notNullRule("id")
    assert(eval(rule, "id", null) === false)
    assert(eval(rule, "id", 42) === true)
    assert(eval(rule, "id", "") === true)
    assert(eval(rule, "id", "42") === true)
  }

  test("regexpRule") {
    val rule = regexpRule("value", "(foo|\\\\d+)")
    assert(eval(rule, "value", null) === false)
    assert(eval(rule, "value", "foo") === true)
    assert(eval(rule, "value", "bar") === false)
    assert(eval(rule, "value", "42") === true)
    assert(eval(rule, "value", 42) === true)
  }

  test("typeRule") {
    val dateRule = typeRule("dt", StringType, DateType)
    assert(eval(dateRule, "dt", null) === true)
    assert(eval(dateRule, "dt", "foo") === false)
    assert(eval(dateRule, "dt", "2020-02-20") === true)
    assert(eval(dateRule, "dt", "2020") === true)
    assert(eval(dateRule, "dt", "42") === false)
    assert(eval(dateRule, "dt", "02/20/2020") === false)

    val customFormatRule = typeRule("dt", StringType, DateType, format = "MM/dd/yyyy")
    assert(eval(customFormatRule, "dt", "02/20/2020") === true)

    val integerToDateRule = typeRule("dt", IntegerType, DateType)
    assert(eval(integerToDateRule, "dt", null) === false)

    val customFormatIntegerRule = typeRule("i", StringType, IntegerType, format = "ignored")
    assert(eval(customFormatIntegerRule, "i", "42") === true)
  }

  test("uniqueRule") {
    val rule = uniqueRule("id")
    assert(eval(rule, Seq(1, 2, 3, 4).toDF("id")) === true)
    assert(eval(rule, Seq(42, 42).toDF("id")) === false)
  }

  test("extras") {
    val customDateRule = namedRule("dt", "is_date", "dt IS NULL OR TO_DATE(dt, 'MM/dd/yyyy') IS NOT NULL")
    assert(eval(customDateRule, "dt", "02/20/2020") === true)

    val multiColumnRule = rule("a_lte_b", "a <= b")
    assert(eval(multiColumnRule, Seq((0, 0)).toDF("a", "b")) === true)
    assert(eval(multiColumnRule, Seq((0, 1)).toDF("a", "b")) === true)
    assert(eval(multiColumnRule, Seq((1, 0)).toDF("a", "b")) === false)
  }

  // TODO: Find better (and faster) way to test expressions.
  private def eval(rule: DqRule, df: DataFrame): Boolean = {
    df.selectExpr(rule.expression).collect().head.get(0).asInstanceOf[Boolean]
  }

  private def eval[V: TypeTag](rule: DqRule, key: String, value: V): Boolean = {
    eval(rule, sparkSession.createDataFrame(Seq(Tuple1(value))).toDF(key))
  }
}
