package io.stoys.spark.aggsum

import io.stoys.spark.SToysException
import io.stoys.spark.SqlUtils.{assertQueryLogicalPlan, getReferencedColumnNames, getReferencedRelationshipNames}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.functions.{collect_list, struct, to_json, typedLit}
import org.apache.spark.sql.types.{BooleanType, DateType, NumericType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.reflect.runtime.universe._

class AggSum[T] private(ds: Dataset[T]) {
  private var config: AggSumConfig = AggSumConfig.default
  private var keyColumnNames: Seq[String] = Seq.empty
  private var referencedTableNames: Seq[String] = Seq.empty

  def config(config: AggSumConfig): AggSum[T] = {
    this.config = config
    this
  }

  def keyColumnNames(keyColumnNames: Seq[String]): AggSum[T] = {
    this.keyColumnNames = keyColumnNames
    this
  }

  def referencedTableNames(referencedTableNames: Seq[String]): AggSum[T] = {
    this.referencedTableNames = referencedTableNames
    this
  }

  def getAggSumInfo: AggSumInfo = {
    val aggSumInfo = if (keyColumnNames.isEmpty) {
      AggSum.getAggSumInfo(ds)
    } else {
      AggSumInfo(
        key_column_names = keyColumnNames,
        value_column_names = ds.columns.filterNot(keyColumnNames.toSet).toSeq,
        referenced_table_names = getReferencedRelationshipNames(ds.queryExecution.logical),
      )
    }
    if (referencedTableNames.nonEmpty) {
      aggSumInfo.copy(referenced_table_names = referencedTableNames)
    } else {
      aggSumInfo
    }
  }

  def computeAggSum(): Dataset[T] = {
    ds
  }

  def computeAggSumResult(): Dataset[AggSumResult] = {
    import ds.sparkSession.implicits._
    val aggSum = config.limit.map(ds.limit).getOrElse(ds)
    val aggSumResult = aggSum.groupBy().agg(
      typedLit(getAggSumInfo).as("aggsum_info"),
      to_json(collect_list(struct(aggSum("*")))).as("data_json"),
    )
    aggSumResult.as[AggSumResult]
  }
}

object AggSum {
  def fromDataFrame(df: DataFrame): AggSum[Row] = {
    fromDataset(df)
  }

  def fromDataset[T: TypeTag](ds: Dataset[T]): AggSum[T] = {
    new AggSum(ds)
  }

  def fromSql(sparkSession: SparkSession, sql: String): AggSum[Row] = {
    new AggSum(sparkSession.sql(sql))
  }

  private def getAggSumInfo(ds: Dataset[_]): AggSumInfo = {
    assertQueryLogicalPlan(ds.queryExecution.logical)
    findTopLevelAggregate(ds.queryExecution.analyzed) match {
      case None =>
        throw new SToysException("Aggregate statement required! (SELECT ... FROM ... GROUP BY ...)")
      case Some(aggregate) =>
        val keyColumnNames = aggregate.groupingExpressions.flatMap(getReferencedColumnNames)
        val aggregateExpressions = aggregate.aggregateExpressions.filter(isAggregateExpression)
        val nonNumericalAggregateExpressions = aggregateExpressions.filterNot(isNumericalExpression)
        if (nonNumericalAggregateExpressions.nonEmpty) {
          val msg = nonNumericalAggregateExpressions.map(_.sql).mkString("[\"", "\", \"", "\"]")
          throw new SToysException(s"Aggregates have to be numerical! Not true for expressions: $msg")
        }
        val valueColumnNames = aggregateExpressions.map(_.name)
        val referencedRelationshipNames = getReferencedRelationshipNames(ds.queryExecution.logical)
        AggSumInfo(keyColumnNames, valueColumnNames, referencedRelationshipNames)
    }
  }

  private def findTopLevelAggregate(logicalPlan: LogicalPlan): Option[Aggregate] = {
    logicalPlan match {
      case a: Aggregate => Some(a)
      case lp => lp.children.flatMap(findTopLevelAggregate).headOption
    }
  }

  private def isNumericalExpression(expression: Expression): Boolean = {
    expression.dataType match {
      case BooleanType => true
      case DateType => true
      case TimestampType => true
      case _: NumericType => true
      case _ => false
    }
  }

  def isAggregateExpression(expression: Expression): Boolean = {
    // Note: In Spark 3.2+ this function can be written as: expression.containsPattern(AGGREGATE_EXPRESSION)
    val aggregateExpression = expression.find {
      case _: AggregateExpression => true
      case _ => false
    }
    aggregateExpression.isDefined
  }
}
