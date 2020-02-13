package com.nuna.trustdb.core.sql

import com.nuna.trustdb.core.SparkTestBase
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

    val taggedValue = Seq(
      TaggedValue(1, Seq("odd")),
      TaggedValue(2, Seq("even")),
      TaggedValue(3, Seq("odd")))
    val params = SquareParams(tag = "odd")
    assert(squares(taggedValue.toDS(), params).collect() === Seq(Square(1, 1), Square(3, 9)))
  }
}

object SparkSqlRunnerTest {
  case class SquareParams(tag: String)
  case class TaggedValue(value: Int, tags: Seq[String])
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
