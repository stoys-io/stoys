package io.stoys.spark.dq

import java.util.Locale

import io.stoys.spark.SToysException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class Dq(sparkSession: SparkSession) {
  import Dq._
  import sparkSession.implicits._

  def dqDataset[T](ds: Dataset[T], rules: Seq[DqRule], config: DqConfig = DqConfig.default,
      metadata: Map[String, String] = Map.empty): Dataset[DqResult] = {
    val ruleInfo = getRuleInfo(ds.columns, rules)
    val wideDqDf = computeWideDqDf(ds, ruleInfo)
    computeDqResult(wideDqDf, ds.columns, ruleInfo, config, metadata)
  }

  def dqTable(tableName: String, rules: Seq[DqRule], config: DqConfig = DqConfig.default,
      metadata: Map[String, String] = Map.empty): Dataset[DqResult] = {
    if (!sparkSession.catalog.tableExists(tableName)) {
      throw new SToysException(s"Table '$tableName' does not exist in current spark session.")
    }
    dqDataset(sparkSession.table(tableName), rules, config, metadata ++ Map("table_name" -> tableName))
  }

  def dqSql(dqSql: String, config: DqConfig = DqConfig.default,
      metadata: Map[String, String] = Map.empty): Dataset[DqResult] = {
    val parsedDqSql = DqSql.parseDqSql(sparkSession, dqSql)
    val missingReferencedTableNames = parsedDqSql.referencedTableNames.filterNot(sparkSession.catalog.tableExists)
    if (missingReferencedTableNames.nonEmpty) {
      throw new SToysException(s"Dq sql reference missing tables: ${missingReferencedTableNames.toList}")
    }
    val wideDqDf = sparkSession.sql(dqSql)
    val columnNames = wideDqDf.columns.dropRight(parsedDqSql.rules.size)
    val ruleInfo = getRuleInfo(columnNames, parsedDqSql.rules)
    computeDqResult(wideDqDf, columnNames, ruleInfo, config, metadata)
  }

  def dqFile(inputPath: String, rules: Seq[DqRule], fields: Seq[DqField], primaryKeyFieldNames: Seq[String],
      config: DqConfig = DqConfig.default, metadata: Map[String, String] = Map.empty): Dataset[DqResult] = {
    val fileInput = DqFile.openFileInputPath(sparkSession, inputPath)
    val schemaRules = DqSchema.generateSchemaRules(fileInput.df.schema, fields, primaryKeyFieldNames, config)
    dqDataset(fileInput.df, rules ++ schemaRules ++ fileInput.rules, config, metadata ++ fileInput.metadata)
  }

  def dqFileViolationPerRow(inputPath: String, rules: Seq[DqRule], fields: Seq[DqField],
      primaryKeyFieldNames: Seq[String], config: DqConfig = DqConfig.default): Dataset[DqViolationPerRow] = {
    val fileInput = DqFile.openFileInputPath(sparkSession, inputPath)
    val schemaRules = DqSchema.generateSchemaRules(fileInput.df.schema, fields, primaryKeyFieldNames, config)
    val ruleInfo = getRuleInfo(fileInput.df.columns, rules ++ schemaRules ++ fileInput.rules)
    val wideDqDf = computeWideDqDf(fileInput.df, ruleInfo)
    computeDqViolationPerRow(wideDqDf, ruleInfo, primaryKeyFieldNames)
  }

  private def getRuleInfo(columnNames: Seq[String], rules: Seq[DqRule]): Seq[RuleInfo] = {
    val normalizedColumnNames = columnNames.map(_.toLowerCase(Locale.ROOT))
    rules.map { rule =>
      val parsedReferencedColumnNames = DqSql.parseReferencedColumnNames(sparkSession, rule.expression)
      // TODO: Solve table aliases correctly. (field name normalization)
      val allReferencedColumnNames = (rule.referenced_column_names ++ parsedReferencedColumnNames)
          .map(_.replace("`", "").split('.').last.toLowerCase(Locale.ROOT))
          .distinct.sorted
      val existingReferencedColumnNames = allReferencedColumnNames.filter(normalizedColumnNames.contains)
      RuleInfo(
        rule = rule,
        allReferencedColumnNames = allReferencedColumnNames,
        allReferencedColumnNamesExist = allReferencedColumnNames.size == existingReferencedColumnNames.size,
        existingReferencedColumnNames = existingReferencedColumnNames,
        existingReferencedColumnIndexes = existingReferencedColumnNames.map(normalizedColumnNames.indexOf)
      )
    }
  }

  private def computeWideDqDf[T](ds: Dataset[T], ruleInfo: Seq[RuleInfo]): DataFrame = {
    val rulesExprs = ruleInfo.map {
      case ri if !ri.allReferencedColumnNamesExist => s"false AS ${ri.rule.name}"
      case ri => s"${ri.rule.expression} AS ${ri.rule.name}"
    }
    ds.selectExpr("*" +: rulesExprs: _*)
  }

  private def checkRuleColumnsSanity(wideDqDf: DataFrame, ruleInfo: Seq[RuleInfo]): Unit = {
    val ruleFields = wideDqDf.schema.fields.takeRight(ruleInfo.size).toSeq

    val nonBooleanRuleFields = ruleFields.filter(_.dataType != BooleanType)
    if (nonBooleanRuleFields.nonEmpty) {
      val nonBooleanRulesMsg = nonBooleanRuleFields.map(f => s"${f.name}: ${f.dataType}").mkString(", ")
      throw new SToysException(s"Dq rules have to return boolean values! Not true for: $nonBooleanRulesMsg.")
    }

    val nonUniqueRuleFields = ruleFields.map(_.name).groupBy(identity).mapValues(_.size).filter(_._2 > 1)
    if (nonUniqueRuleFields.nonEmpty) {
      val nonUniqueRulesMsg = nonUniqueRuleFields.toSeq.sorted.map(kv => s"${kv._1}: ${kv._2}x").mkString(", ")
      throw new SToysException(s"Dq rules have to have unique names! Not true for: $nonUniqueRulesMsg.")
    }
  }

  private def computeDqResult(wideDqDf: DataFrame, columnNames: Seq[String], ruleInfo: Seq[RuleInfo],
      config: DqConfig, metadata: Map[String, String]): Dataset[DqResult] = {
    checkRuleColumnsSanity(wideDqDf, ruleInfo)
    val ruleHashesExprs = ruleInfo.map {
      case ri if !ri.allReferencedColumnNamesExist => expr(s"42 AS ${ri.rule.name}")
      case ri if ri.allReferencedColumnNames.isEmpty => expr(s"IF(${ri.rule.name}, -1, 42) AS ${ri.rule.name}")
      case ri =>
        val hashExpr = s"HASH(${ri.allReferencedColumnNames.mkString(", ")}, 42)"
        expr(s"IF(${ri.rule.name}, -1, ABS($hashExpr)) AS ${ri.rule.name}")
    }
    val dqAggInputRowDf = wideDqDf.select(
//      col("*"),
//      struct(columnNames.map(col): _*).as("row"),
      array(columnNames.map(cn => col(cn).cast(StringType)): _*).as("rowSample"),
      monotonically_increasing_id().as("rowId"),
      array(ruleHashesExprs: _*).as("ruleHashes")
    )
    val existingReferencedColumnIndexes = ruleInfo.map(_.existingReferencedColumnIndexes)
    val aggregator = new DqAggregator(columnNames.size, existingReferencedColumnIndexes, config)
    val dqAggOutputRowDs = dqAggInputRowDf.as[DqAggregator.DqAggInputRow].select(aggregator.toColumn)

    dqAggOutputRowDs.map { aggOutputRow =>
      val ruleNames = ruleInfo.map(_.rule.name)
      val resultColumns = columnNames.map(cn => DqColumn(cn))
      val resultRules = ruleInfo.map(ri => ri.rule.copy(referenced_column_names = ri.allReferencedColumnNames))
      val statistics = DqStatistics(
        DqTableStatistic(aggOutputRow.rows, aggOutputRow.rowViolations),
        columnNames.zip(aggOutputRow.columnViolations).map(DqColumnStatistics.tupled),
        ruleNames.zip(aggOutputRow.ruleViolations).map(DqRuleStatistics.tupled)
      )
      val rowSample = aggOutputRow.rowSample.zip(aggOutputRow.ruleHashes).map {
        case (rowSample, ruleHashes) => DqRowSample(rowSample, ruleHashes.zip(ruleNames).filter(_._1 >= 0).map(_._2))
      }
      DqResult(resultColumns, resultRules, statistics, rowSample, metadata)
    }
  }

  private def computeDqViolationPerRow(wideDqDf: DataFrame, ruleInfo: Seq[RuleInfo],
      primaryKeyFieldNames: Seq[String]): Dataset[DqViolationPerRow] = {
    checkRuleColumnsSanity(wideDqDf, ruleInfo)
    val stackExprs = ruleInfo.map { ri =>
      val column = array(ri.existingReferencedColumnNames.map(lit): _*)
      val value = array(ri.existingReferencedColumnNames.map(cn => col(cn).cast(StringType)): _*)
      Seq(col(ri.rule.name), column, value, lit(ri.rule.name), lit(ri.rule.expression))
    }
    val stackExpr = stackExprs.flatten.map(_.expr.sql).mkString(", ")
    val stackedDf = wideDqDf.select(
      array(primaryKeyFieldNames.map(col): _*).as("primary_key"),
      expr(s"STACK(${stackExprs.size}, $stackExpr) AS (result, column_names, values, rule_name, rule_expression)")
    )
    val filteredDf = stackedDf.where(not(col("result"))).drop("result")
    filteredDf.as[DqViolationPerRow]
  }
}

object Dq {
  private case class RuleInfo(
      rule: DqRule,
      allReferencedColumnNames: Seq[String],
      allReferencedColumnNamesExist: Boolean,
      existingReferencedColumnNames: Seq[String],
      existingReferencedColumnIndexes: Seq[Int]
  )
}
