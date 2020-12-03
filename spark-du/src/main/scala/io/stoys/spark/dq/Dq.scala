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
    computeDqResult(computeWideDf(ds, rules), rules, config, metadata)
  }

  def dqTable(tableName: String, rules: Seq[DqRule], config: DqConfig = DqConfig.default,
      metadata: Map[String, String] = Map.empty): Dataset[DqResult] = {
    if (!sparkSession.catalog.tableExists(tableName)) {
      throw new SToysException(s"Table '$tableName' does not exist in current spark session.")
    }
    dqDataset(sparkSession.table(tableName), rules, config, metadata)
  }

  def dqSql(dqSql: String, config: DqConfig = DqConfig.default,
      metadata: Map[String, String] = Map.empty): Dataset[DqResult] = {
    val parsedDqSql = DqSql.parseDqSql(sparkSession, dqSql)
    val missingReferencedTableNames = parsedDqSql.referencedTableNames.filterNot(sparkSession.catalog.tableExists)
    if (missingReferencedTableNames.nonEmpty) {
      throw new SToysException(s"Dq sql reference missing tables: ${missingReferencedTableNames.toList}")
    }
    computeDqResult(sparkSession.sql(dqSql), parsedDqSql.rules, config, metadata)
  }

  def dqFile(inputPath: String, rules: Seq[DqRule], fields: Seq[DqField], primaryKeyFieldNames: Seq[String],
      config: DqConfig = DqConfig.default, metadata: Map[String, String] = Map.empty): Dataset[DqResult] = {
    val fileInput = DqFile.openFileInputPath(sparkSession, inputPath)
    val schemaRules = DqSchema.generateSchemaRules(fileInput.df.schema, fields, primaryKeyFieldNames, config)
    dqDataset(fileInput.df, rules ++ schemaRules ++ fileInput.rules, config, metadata ++ fileInput.metadata)
  }

  private def getRuleReferences(columnNames: Seq[String], rules: Seq[DqRule]): Seq[RuleReferences] = {
    val normalizedColumnNames = columnNames.map(_.toLowerCase(Locale.ROOT))
    rules.map { rule =>
      val parsedReferencedColumnNames = DqSql.parseReferencedColumnNames(sparkSession, rule.expression)
      val allReferencedColumnNames = rule.referenced_column_names ++ parsedReferencedColumnNames
      // TODO: Solve table aliases correctly. (sne field name normalization)
      val normalizedReferencedColumnNames = allReferencedColumnNames
          .map(_.replace("`", "").split('.').last.toLowerCase(Locale.ROOT))
          .distinct.sorted
      val columnIndexes = normalizedReferencedColumnNames.map(normalizedColumnNames.indexOf)
      RuleReferences(rule, normalizedReferencedColumnNames, columnIndexes.filter(_ != -1), !columnIndexes.contains(-1))
    }
  }

  private def computeDqResult(wideDf: DataFrame, rules: Seq[DqRule],
      config: DqConfig, metadata: Map[String, String]): Dataset[DqResult] = {
    val wideDfFields = wideDf.schema.fields.toSeq
    val (dataFields, ruleFields) = wideDfFields.splitAt(wideDfFields.size - rules.size)
    val columnNames = dataFields.map(_.name)
    val ruleNames = ruleFields.map(_.name)

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

    val ruleReferences = getRuleReferences(columnNames, rules)
    val ruleHashesExprs = ruleReferences.map {
      case rr if !rr.allColumnsExist => expr(s"42 AS ${rr.rule.name}")
      case rr if rr.columnNames.isEmpty => expr(s"IF(${rr.rule.name}, -1, 42) AS ${rr.rule.name}")
      case rr => expr(s"IF(${rr.rule.name}, -1, ABS(HASH(${rr.columnNames.mkString(", ")}, 42))) AS ${rr.rule.name}")
    }
    val dqAggInputRowDf = wideDf.select(
//      col("*"),
//      struct(columnNames.map(col): _*).as("row"),
      array(columnNames.map(cn => col(cn).cast(StringType)): _*).as("rowSample"),
      monotonically_increasing_id().as("rowId"),
      array(ruleHashesExprs: _*).as("ruleHashes")
    )
    val referencedColumnIndexes = ruleReferences.map(_.validColumnIndexes)
    val aggregator = new DqAggregator(columnNames.size, ruleNames.size, referencedColumnIndexes, config)
    val dqAggOutputRowDs = dqAggInputRowDf.as[DqAggregator.DqAggInputRow].select(aggregator.toColumn)

    dqAggOutputRowDs.map { aggOutputRow =>
      val resultColumns = columnNames.map(cn => DqColumn(cn))
      val resultRules = ruleReferences.map(rr => rr.rule.copy(referenced_column_names = rr.columnNames))
      val statistics = DqStatistics(
        DqTableStatistic(aggOutputRow.rows, aggOutputRow.rowViolations),
        columnNames.zip(aggOutputRow.columnViolations).map(ca => DqColumnStatistics(ca._1, ca._2)),
        ruleNames.zip(aggOutputRow.ruleViolations).map(ra => DqRuleStatistics(ra._1, ra._2))
      )
      val rowSample = aggOutputRow.rowSample.zip(aggOutputRow.violatedRuleIndexes).map {
        case (rowSample, violatedRuleIndexes) => DqRowSample(rowSample, violatedRuleIndexes.map(ruleNames))
      }
      DqResult(resultColumns, resultRules, statistics, rowSample, metadata)
    }
  }

  private def computeWideDf[T](ds: Dataset[T], rules: Seq[DqRule]): DataFrame = {
    val ruleReferences = getRuleReferences(ds.columns, rules)
    val rulesExprs = ruleReferences.map {
      case ruleReference if !ruleReference.allColumnsExist => s"false AS ${ruleReference.rule.name}"
      case ruleReference => s"${ruleReference.rule.expression} AS ${ruleReference.rule.name}"
    }
    ds.selectExpr(Seq("*") ++ rulesExprs: _*)
  }
}

object Dq {
  private case class RuleReferences(
      rule: DqRule, columnNames: Seq[String], validColumnIndexes: Seq[Int], allColumnsExist: Boolean)
}
