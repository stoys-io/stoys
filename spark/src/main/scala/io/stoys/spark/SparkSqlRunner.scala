package io.stoys.spark

import io.stoys.scala.{IO, Strings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.runtime.universe._

object SparkSqlRunner {
  import io.stoys.spark.implicits._

  val SPECIAL_COLUMN_NAMES_PATTERN = "__(.*)__"
  val LABELS_SPECIAL_COLUMN_NAME = "__labels__"

  private[spark] def splitSqlStatements(databricksNotebookText: String): Seq[String] = {
    databricksNotebookText.split("-- COMMAND --").head.split(';').toSeq.map(_.trim).filterNot(_.isEmpty)
  }

  /**
   * Run spark sql query from text file stored at classpath.
   *
   * @see [[runSql]]
   */
  def runSqlDF(sparkSession: SparkSession, clazz: Class[_], sqlFileName: String, tables: Map[String, Dataset[_]],
      params: Option[Product] = None): DataFrame = {
    val rawSqlText = IO.resourceToString(clazz, sqlFileName)
    val sqlText = Strings.replaceParams(rawSqlText, params)
    val sqlStatements = splitSqlStatements(sqlText)

    tables.foreach {
      case (name, table) =>
        table.createOrReplaceTempView(name)
    }
    val result = sqlStatements.map(sparkSession.sql).last
    tables.keys.foreach(sparkSession.catalog.dropTempView)
    result
  }

  /**
   * Run spark sql query from text file stored at classpath.
   *
   * @param sparkSession Instance of [[SparkSession]]
   * @param clazz Runtime [[Class]] that is used to get jar and package name that serve
   * as base directory for sqlFileName. Just use "this.getClass" at call site.
   * @param sqlFileName File name of the sql query.
   * @param tables Map from dataset (or dataframe) name to the actual [[Dataset]] (or [[DataFrame]]).
   * The names are the names of tables referenced in the sql.
   * @param params Optional parameter with sql parameters. All the params should be in one case class (or proto).
   * @tparam U Output schema.
   * @return [[Dataset]] of output records.
   */
  def runSql[U <: Product : TypeTag](sparkSession: SparkSession, clazz: Class[_], sqlFileName: String,
      tables: Map[String, Dataset[_]], params: Option[Product] = None): Dataset[U] = {
    runSqlDF(sparkSession, clazz, sqlFileName, tables, params).reshape[U]
  }

  /**
   * Run spark sql query to compute metrics. The query is expected to select single row where column name is metric name
   * and value is [[Double]] (or implicitly convertible to it). This method will then transpose the result from one row
   * with many columns to [[Dataset]] of [[Metric]].
   *
   * Note: It is also adding "class_name" label taken from clazz argument.
   *
   * A few special columns are supported:
   * 1) __labels__ column value (map type) will be merged with other labels
   *
   * @return [[Dataset]] of [[Metric]]
   */
  def runSqlMetric(sparkSession: SparkSession, clazz: Class[_], sqlFileName: String, tables: Map[String, Dataset[_]],
      params: Option[Product] = None, labels: Map[String, String] = Map.empty): Dataset[Metric] = {
    import sparkSession.implicits._

    val metricsDf = runSqlDF(sparkSession, clazz, sqlFileName, tables, params)
    val (specialColumnNames, regularColumnNames) =
      metricsDf.columns.toSeq.partition(_.matches(SPECIAL_COLUMN_NAMES_PATTERN))
    val metricColumnsTransposed = explode(map(regularColumnNames.flatMap(c => Array(lit(c), col(c))): _*))
    var df = metricsDf.select(specialColumnNames.map(col) :+ metricColumnsTransposed: _*)
    val staticLabelsColumn = typedLit(Map("class_name" -> clazz.getSimpleName) ++ labels)
    if (specialColumnNames.contains(LABELS_SPECIAL_COLUMN_NAME)) {
      df = df.withColumn("labels", map_concat(staticLabelsColumn, col(LABELS_SPECIAL_COLUMN_NAME)))
    } else {
      df = df.withColumn("labels", staticLabelsColumn)
    }
    df = df.drop(specialColumnNames: _*)
    df.as[Metric]
  }
}
