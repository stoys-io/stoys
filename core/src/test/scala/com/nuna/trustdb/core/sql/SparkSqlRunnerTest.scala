package com.nuna.trustdb.core.sql

import com.nuna.trustdb.core.{Metric, SparkTestBase}
import org.apache.spark.sql.Dataset

class SparkSqlRunnerTest extends SparkTestBase {
  import SparkSqlRunnerTest._
  import com.nuna.trustdb.core.sql.SparkSqlRunner._
  import sparkSession.implicits._

  test("splitSqlStatements") {
    assert(splitSqlStatements(createNotebookText("")) === Seq.empty)
    assert(splitSqlStatements(createNotebookText("SELECT * FROM foo;")) === Seq("SELECT * FROM foo"))
    assert(splitSqlStatements(createNotebookText("foo;\nbar;")) === Seq("foo", "bar"))
  }

  test("runSql") {
    def squares(taggedValue: Dataset[TaggedValue], params: SquareParams): Dataset[Square] = {
      val inputs = Map("tagged_value" -> taggedValue)
      runSql[Square](sparkSession, this.getClass, "square_test.sql", inputs, Some(params))
    }

    val taggedValue = Seq(TaggedValue(1, "odd"), TaggedValue(2, "even"), TaggedValue(3, "odd"))
    val params = SquareParams(tag = "odd")
    assert(squares(taggedValue.toDS(), params).collect() === Seq(Square(1, 1), Square(3, 9)))
  }

  test("runSqlMetric") {
    val taggedValue = Seq(TaggedValue(1, "odd"), TaggedValue(2, "even"), TaggedValue(3, "odd"))
    val inputs = Map("tagged_value" -> taggedValue.toDS())
    val labels = Map("foo" -> "bar")
    val actualMetrics = runSqlMetric(sparkSession, this.getClass, "metrics_test.sql", inputs, labels = labels)
    val defaultLabels = Map("class_name" -> this.getClass.getSimpleName)
    val expectedMetrics = Seq(
      Metric("count__odd", 2.0, defaultLabels ++ labels ++ Map("grouped_by" -> "odd")),
      Metric("avg_value__odd", 2.0, defaultLabels ++ labels ++ Map("grouped_by" -> "odd")),
      Metric("count__even", 1.0, defaultLabels ++ labels ++ Map("grouped_by" -> "even")),
      Metric("avg_value__even", 2.0, defaultLabels ++ labels ++ Map("grouped_by" -> "even")),
    )
    assert(actualMetrics.collect() === expectedMetrics)
  }
}

object SparkSqlRunnerTest {
  case class SquareParams(tag: String)
  case class TaggedValue(value: Int, tag: String)
  case class Square(value: Int, squared_value: Int)
  case class SimpleRow(str_value: String)

  val databricksNotebookTextTemplate =
    """
      |%s
      |
      |-- COMMAND ----------
      |
      |/*
      |-- other cells
      |SELECT * FROM my_input;
      |*/
    """.stripMargin

  def createNotebookText(sqlText: String): String = {
    databricksNotebookTextTemplate.format(sqlText)
  }
}
