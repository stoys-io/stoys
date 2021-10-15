package io.stoys.spark.dq

import io.stoys.spark.{SToysException, SqlUtils}
import org.apache.spark.sql.catalyst.expressions.Stack
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import scala.collection.mutable

private[dq] object DqFramework {
  val ROW_NUMBER_FIELD_NAME = "__row_number__"
  val MISSING_TOKEN = "__MISSING__"
  val NULL_TOKEN = "__NULL__"

  case class ColumnNamesInfo(
      all: Seq[String],
      existing: Seq[String],
      existingIndexes: Seq[Int],
      missing: Seq[String],
  )

  case class RuleInfo(
      rule: DqRule,
      columnNamesInfo: ColumnNamesInfo,
  )

  private def getColumnNamesInfo(columnNames: Seq[String], referencedColumnNames: Seq[String]): ColumnNamesInfo = {
    val indexesByNormalizedNames = columnNames.zipWithIndex.map(ci => ci._1.toLowerCase -> ci._2).toMap
    val visitedNames = mutable.Set.empty[String]
    val (missingNames, existingNames, existingIndexes) = referencedColumnNames.map({ name =>
      val normalizedName = name.toLowerCase
      if (visitedNames.contains(normalizedName)) {
        (None, None, None)
      } else {
        visitedNames.add(normalizedName)
        indexesByNormalizedNames.get(normalizedName) match {
          case Some(index) => (None, Some(columnNames(index)), Some(index))
          case None => (Some(name), None, None)
        }
      }
    }).unzip3
    val allNames = existingNames ++ missingNames
    ColumnNamesInfo(allNames.flatten, existingNames.flatten, existingIndexes.flatten, missingNames.flatten)
  }

  def getRuleInfo(sparkSession: SparkSession, columnNames: Seq[String], rules: Seq[DqRule]): Seq[RuleInfo] = {
    rules.map { rule =>
      val explicitRawNames = Option(rule.referenced_column_names).getOrElse(Seq.empty)
      val expression = sparkSession.sessionState.sqlParser.parseExpression(rule.expression)
      val parsedRawNames = SqlUtils.getReferencedColumnNames(expression)
      val rawNames = explicitRawNames ++ parsedRawNames
      // TODO: Solve table aliases correctly. (field name normalization)
      val correctedRawNames = rawNames.map(_.replace("`", "").split('.').last)
      val columnNamesInfo = getColumnNamesInfo(columnNames, correctedRawNames)
      RuleInfo(rule, columnNamesInfo)
    }
  }

  def checkWideDqColumnsSanity(wideDqSchema: StructType, ruleCount: Int): Boolean = {
    val ruleFields = wideDqSchema.fields.takeRight(ruleCount)

    val nonBooleanRuleFields = ruleFields.filter(_.dataType != BooleanType)
    if (nonBooleanRuleFields.nonEmpty) {
      val nonBooleanRulesMsg = nonBooleanRuleFields.map(f => s"${f.name}: ${f.dataType}").mkString(", ")
      throw new SToysException(s"Dq rules have to return boolean values! Not true for: $nonBooleanRulesMsg.")
    }

    val nonUniqueFields = wideDqSchema.fields.map(_.name).groupBy(_.toLowerCase).filter(_._2.length > 1)
    if (nonUniqueFields.nonEmpty) {
      val nonUniqueRulesMsg = nonUniqueFields.toSeq.map(kv => s"${kv._1}: ${kv._2.length}x").sorted.mkString(", ")
      throw new SToysException(s"Dq rules and fields have to have unique names! Not true for: $nonUniqueRulesMsg.")
    }

    true
  }

  case class WideDqDfInfo(
      wideDqDf: DataFrame,
      columnNames: Seq[String],
      ruleInfo: Seq[RuleInfo],
  )

  def computeWideDqDfInfo[T](ds: Dataset[T], rulesWithinDs: Seq[DqRule], rules: Seq[DqRule]): WideDqDfInfo = {
    val columnNames = ds.columns.toSeq.dropRight(rulesWithinDs.size)
    val ruleInfoWithinDs = getRuleInfo(ds.sparkSession, columnNames, rulesWithinDs)
    val ruleInfo = getRuleInfo(ds.sparkSession, columnNames, rules)
    val ruleInfoCombined = ruleInfoWithinDs ++ ruleInfo
    val wideDqDf = if (ruleInfo.isEmpty) ds.toDF() else computeWideDqDf(ds, ruleInfo)
    checkWideDqColumnsSanity(wideDqDf.schema, ruleInfoCombined.size)
    WideDqDfInfo(wideDqDf, columnNames, ruleInfoCombined)
  }

  private def computeWideDqDf[T](ds: Dataset[T], ruleInfo: Seq[RuleInfo]): DataFrame = {
    val rulesExprs = ruleInfo.map {
      case ri if ri.columnNamesInfo.missing.nonEmpty => lit(false).as(ri.rule.name)
      case ri => expr(ri.rule.expression).as(ri.rule.name)
    }
    ds.select(col("*") +: rulesExprs: _*)
  }

  def computeDqResult(wideDqDfInfo: WideDqDfInfo, config: DqConfig,
      metadata: Map[String, String]): Dataset[DqResult] = {
    import wideDqDfInfo.wideDqDf.sparkSession.implicits._

    val ruleHashesExprs = wideDqDfInfo.ruleInfo.map {
      case ri if ri.columnNamesInfo.missing.nonEmpty => lit(42).as(ri.rule.name)
      case ri if ri.columnNamesInfo.existing.isEmpty =>
        when(col(ri.rule.name), lit(-1)).otherwise(lit(42)).as(ri.rule.name)
      case ri =>
        val hashExpr = hash(ri.columnNamesInfo.existing.map(col) :+ lit(42): _*)
        when(col(ri.rule.name), lit(-1)).otherwise(abs(hashExpr)).as(ri.rule.name)
    }
    val dqAggInputRowDf = wideDqDfInfo.wideDqDf.select(
      monotonically_increasing_id().as("rowId"),
//      struct(wideDqDfInfo.columnNames.map(col): _*).as("row"),
      array(wideDqDfInfo.columnNames.map(cn => col(cn).cast(StringType)): _*).as("row"),
      array(ruleHashesExprs: _*).as("ruleHashes"),
    )
    val existingReferencedColumnIndexes = wideDqDfInfo.ruleInfo.map(_.columnNamesInfo.existingIndexes)
    val columnCount = wideDqDfInfo.columnNames.size
    val ruleCount = wideDqDfInfo.ruleInfo.size
    val partitionCount = dqAggInputRowDf.rdd.getNumPartitions
    val sensibleRowNumber = metadata.contains("file_name")
    val aggregator = new DqAggregator(columnCount, ruleCount, partitionCount, sensibleRowNumber,
      existingReferencedColumnIndexes, config)
    val aggOutputRowDs = dqAggInputRowDf.as[DqAggregator.DqAggInputRow].select(aggregator.toColumn)

    aggOutputRowDs.map { aggOutputRow =>
      val ruleNames = wideDqDfInfo.ruleInfo.map(_.rule.name)
      val columnNames = wideDqDfInfo.columnNames ++ (if (sensibleRowNumber) Option(ROW_NUMBER_FIELD_NAME) else None)
      val columnViolations = aggOutputRow.columnViolations.toSeq ++ (if (sensibleRowNumber) Option(0L) else None)
      val resultColumns = columnNames.map(cn => DqColumn(cn))
      val resultRules = wideDqDfInfo.ruleInfo.map(ri => ri.rule.copy(referenced_column_names = ri.columnNamesInfo.all))
      val statistics = DqStatistics(
        table = DqTableStatistic(aggOutputRow.rows, aggOutputRow.rowViolations),
        column = columnNames.zip(columnViolations).map(nv => DqColumnStatistics(nv._1, nv._2)),
        rule = ruleNames.zip(aggOutputRow.ruleViolations).map(nv => DqRuleStatistics(nv._1, nv._2)),
      )
      val rowSample = aggOutputRow.rowSample.map { row =>
        DqRowSample(row.row.toSeq, row.ruleHashes.toSeq.zip(ruleNames).filter(_._1 >= 0).map(_._2))
      }
      DqResult(resultColumns, resultRules, statistics, rowSample.toSeq, metadata)
    }
  }

  private def createSafeValuesArrayExpr(columnNamesInfo: ColumnNamesInfo): Column = {
    val existingValues = columnNamesInfo.existing.map(cn => coalesce(col(cn).cast(StringType), lit(NULL_TOKEN)))
    val missingValues = columnNamesInfo.missing.map(_ => lit(MISSING_TOKEN))
    array(existingValues ++ missingValues: _*)
  }

  def computeDqViolationPerRow(wideDqDfInfo: WideDqDfInfo,
      primaryKeyFieldNames: Seq[String]): Dataset[DqViolationPerRow] = {
    import wideDqDfInfo.wideDqDf.sparkSession.implicits._

    val primaryKeyExpr = createSafeValuesArrayExpr(getColumnNamesInfo(wideDqDfInfo.columnNames, primaryKeyFieldNames))
    val stackExprs = wideDqDfInfo.ruleInfo.map { ruleInfo =>
      val result = if (ruleInfo.columnNamesInfo.missing.nonEmpty) lit(false) else col(ruleInfo.rule.name)
      val column = array(ruleInfo.columnNamesInfo.all.map(lit): _*)
      val value = createSafeValuesArrayExpr(ruleInfo.columnNamesInfo)
      Seq(result, column, value, lit(ruleInfo.rule.name), lit(ruleInfo.rule.expression))
    }
    val stackExpr = stack(lit(stackExprs.size) +: stackExprs.flatten)
    val stackedDf = wideDqDfInfo.wideDqDf.select(
      primaryKeyExpr.as("primary_key"),
      stackExpr.as(Seq("result", "column_names", "values", "rule_name", "rule_expression")),
    )
    val filteredDf = stackedDf.where(not(col("result"))).drop("result")
    filteredDf.as[DqViolationPerRow]
  }

  def selectFailingRows(wideDqDf: DataFrame, ruleCount: Int): DataFrame = {
    if (ruleCount > 0) {
      val columns = wideDqDf.columns
      val (columnNames, ruleNames) = columns.splitAt(columns.length - ruleCount)
      wideDqDf.where(not(ruleNames.map(col).reduce(_ and _))).select(columnNames.map(col): _*)
    } else {
      wideDqDf.limit(0)
    }
  }

  def selectPassingRows(wideDqDf: DataFrame, ruleCount: Int): DataFrame = {
    if (ruleCount > 0) {
      val columns = wideDqDf.columns
      val (columnNames, ruleNames) = columns.splitAt(columns.length - ruleCount)
      wideDqDf.where(ruleNames.map(col).reduce(_ and _)).select(columnNames.map(col): _*)
    } else {
      wideDqDf
    }
  }

  private def stack(columns: Seq[Column]): Column = {
    new Column(Stack(columns.map(_.expr)))
  }
}
