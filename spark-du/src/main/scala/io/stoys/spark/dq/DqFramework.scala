package io.stoys.spark.dq

import java.util.Locale

import io.stoys.spark.SToysException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

private[dq] object DqFramework {
  case class RuleInfo(
      rule: DqRule,
      missingReferencedColumnNames: Seq[String],
      existingReferencedColumnNames: Seq[String],
      existingReferencedColumnIndexes: Seq[Int]
  )

  def getRuleInfo(sparkSession: SparkSession, columnNames: Seq[String], rules: Seq[DqRule]): Seq[RuleInfo] = {
    val indexesByNormalizedNames = columnNames.zipWithIndex.map(ci => ci._1.toLowerCase(Locale.ROOT) -> ci._2).toMap
    rules.map { rule =>
      val explicitRawNames = Option(rule.referenced_column_names).getOrElse(Seq.empty)
      val parsedRawNames = DqSql.parseReferencedColumnNames(sparkSession, rule.expression)
      val allRawNames = explicitRawNames ++ parsedRawNames
      val visitedNormalizedRawNames = mutable.Set.empty[String]
      val (missingNames, existingNames, existingIndexes) = allRawNames.map({ rawName =>
        // TODO: Solve table aliases correctly. (field name normalization)
        val correctedRawName = rawName.replace("`", "").split('.').last
        val normalizedRawName = correctedRawName.toLowerCase(Locale.ROOT)
        if (visitedNormalizedRawNames.contains(normalizedRawName)) {
          (None, None, None)
        } else {
          visitedNormalizedRawNames.add(normalizedRawName)
          indexesByNormalizedNames.get(normalizedRawName) match {
            case Some(index) => (None, Some(columnNames(index)), Some(index))
            case None => (Some(correctedRawName), None, None)
          }
        }
      }).unzip3
      RuleInfo(
        rule = rule,
        missingReferencedColumnNames = missingNames.flatten,
        existingReferencedColumnNames = existingNames.flatten,
        existingReferencedColumnIndexes = existingIndexes.flatten
      )
    }
  }

  def checkWideDqColumnsSanity(wideDqSchema: StructType, ruleCount: Int): Boolean = {
    val ruleFields = wideDqSchema.fields.takeRight(ruleCount)

    val nonBooleanRuleFields = ruleFields.filter(_.dataType != BooleanType)
    if (nonBooleanRuleFields.nonEmpty) {
      val nonBooleanRulesMsg = nonBooleanRuleFields.map(f => s"${f.name}: ${f.dataType}").mkString(", ")
      throw new SToysException(s"Dq rules have to return boolean values! Not true for: $nonBooleanRulesMsg.")
    }

    val nonUniqueFields = wideDqSchema.fields.map(_.name).groupBy(_.toLowerCase(Locale.ROOT)).filter(_._2.length > 1)
    if (nonUniqueFields.nonEmpty) {
      val nonUniqueRulesMsg = nonUniqueFields.toSeq.map(kv => s"${kv._1}: ${kv._2.length}x").sorted.mkString(", ")
      throw new SToysException(s"Dq rules and fields have to have unique names! Not true for: $nonUniqueRulesMsg.")
    }

    true
  }

  case class WideDqDfInfo(wideDqDf: DataFrame, ruleInfo: Seq[RuleInfo])

  def computeWideDqDfInfo[T](ds: Dataset[T], rulesWithinDs: Seq[DqRule], rules: Seq[DqRule]): WideDqDfInfo = {
    val columnNames = ds.columns.toSeq.dropRight(rulesWithinDs.size)
    val ruleInfoWithinDs = getRuleInfo(ds.sparkSession, columnNames, rulesWithinDs)
    val ruleInfo = getRuleInfo(ds.sparkSession, columnNames, rules)
    val ruleInfoCombined = ruleInfoWithinDs ++ ruleInfo
    val wideDqDf = if (ruleInfo.isEmpty) ds.toDF() else computeWideDqDf(ds, ruleInfo)
    checkWideDqColumnsSanity(wideDqDf.schema, ruleInfoCombined.size)
    WideDqDfInfo(wideDqDf, ruleInfoCombined)
  }

  private def computeWideDqDf[T](ds: Dataset[T], ruleInfo: Seq[RuleInfo]): DataFrame = {
    val rulesExprs = ruleInfo.map {
      case ri if ri.missingReferencedColumnNames.nonEmpty => s"false AS ${ri.rule.name}"
      case ri => s"${ri.rule.expression} AS ${ri.rule.name}"
    }
    ds.selectExpr("*" +: rulesExprs: _*)
  }

  def computeDqResult(wideDqDf: DataFrame, columnNames: Seq[String], ruleInfo: Seq[RuleInfo],
      config: DqConfig, metadata: Map[String, String]): Dataset[DqResult] = {
    import wideDqDf.sparkSession.implicits._

    val ruleHashesExprs = ruleInfo.map {
      case ri if ri.missingReferencedColumnNames.nonEmpty => expr(s"42 AS ${ri.rule.name}")
      case ri if ri.existingReferencedColumnNames.isEmpty => expr(s"IF(${ri.rule.name}, -1, 42) AS ${ri.rule.name}")
      case ri =>
        val hashExpr = s"HASH(${ri.existingReferencedColumnNames.mkString(", ")}, 42)"
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
    val aggOutputRowDs = dqAggInputRowDf.as[DqAggregator.DqAggInputRow].select(aggregator.toColumn)

    aggOutputRowDs.map { aggOutputRow =>
      val ruleNames = ruleInfo.map(_.rule.name)
      val resultColumns = columnNames.map(cn => DqColumn(cn))
      val resultRules = ruleInfo.map { ri =>
        ri.rule.copy(referenced_column_names = ri.existingReferencedColumnNames ++ ri.missingReferencedColumnNames)
      }
      val statistics = DqStatistics(
        DqTableStatistic(aggOutputRow.rows, aggOutputRow.rowViolations),
        columnNames.zip(aggOutputRow.columnViolations).map(DqColumnStatistics.tupled),
        ruleNames.zip(aggOutputRow.ruleViolations).map(DqRuleStatistics.tupled)
      )
      val rowSample = aggOutputRow.rowSample.zip(aggOutputRow.ruleHashes).toSeq.map {
        case (rowSample, ruleHashes) =>
          DqRowSample(rowSample.toSeq, ruleHashes.toSeq.zip(ruleNames).filter(_._1 >= 0).map(_._2))
      }
      DqResult(resultColumns, resultRules, statistics, rowSample.toSeq, metadata)
    }
  }

  def computeDqViolationPerRow(wideDqDf: DataFrame, ruleInfo: Seq[RuleInfo],
      primaryKeyFieldNames: Seq[String]): Dataset[DqViolationPerRow] = {
    import wideDqDf.sparkSession.implicits._

    val stackExprs = ruleInfo.map { ri =>
      val existingColumnNames = ri.existingReferencedColumnNames
      val missingColumnNames = ri.missingReferencedColumnNames
      val result = if (missingColumnNames.nonEmpty) lit(false) else col(ri.rule.name)
      val column = array((existingColumnNames ++ missingColumnNames).map(lit): _*)
      val existingColumnValues = existingColumnNames.map(cn => coalesce(col(cn).cast(StringType), lit("__NULL__")))
      val missingColumnValues = missingColumnNames.map(_ => lit("__MISSING__"))
      val value = array(existingColumnValues ++ missingColumnValues: _*)
      Seq(result, column, value, lit(ri.rule.name), lit(ri.rule.expression))
    }
    val stackExpr = stackExprs.flatten.map(_.expr.sql).mkString(", ")
    val stackedDf = wideDqDf.select(
      array(primaryKeyFieldNames.map(fn => col(fn).cast(StringType)): _*).as("primary_key"),
      expr(s"STACK(${stackExprs.size}, $stackExpr) AS (result, column_names, values, rule_name, rule_expression)")
    )
    val filteredDf = stackedDf.where(not(col("result"))).drop("result")
    filteredDf.as[DqViolationPerRow]
  }

  def selectFailingRows(wideDqDf: DataFrame, ruleCount: Int): DataFrame = {
    if (ruleCount > 0) {
      val columns = wideDqDf.columns
      val (columnNames, ruleNames) = columns.splitAt(columns.length - ruleCount)
      wideDqDf.where(s"NOT ${ruleNames.mkString("(", " AND ", ")")}").select(columnNames.map(col): _*)
    } else {
      wideDqDf.limit(0)
    }
  }
}
