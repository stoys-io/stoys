package io.stoys.spark.dq

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

class DqAggregator(columnCount: Int, existingReferencedColumnIndexes: Seq[Seq[Int]], config: DqConfig)
    extends Aggregator[DqAggregator.DqAggInputRow, DqAggregator.DqAgg, DqAggregator.DqAggOutputRow] {
  import DqAggregator._

  private val ruleCount = existingReferencedColumnIndexes.size
  private val rowsPerRule = if (config.sample_rows) config.max_rows_per_rule else 0

  override def zero: DqAgg = {
    DqAgg(
      aggPerTable = DqAggPerTable(rows = 0L, violations = 0L),
      aggPerColumn = Array.fill(columnCount)(DqAggPerColumn(violations = 0L)),
      aggPerRule = Array.fill(ruleCount)(DqAggPerRule(
        violations = 0L,
        minHash = if (rowsPerRule <= 0) Long.MinValue else Long.MaxValue,
        hashes = Array.fill(rowsPerRule)(-1L),
        rowIds = Array.fill(rowsPerRule)(-1L),
        rowSample = Array.fill(rowsPerRule)(null),
        ruleHashes = Array.fill(rowsPerRule)(null)))
    )
  }

  override def reduce(agg: DqAgg, row: DqAggInputRow): DqAgg = {
    var ruleViolationsPerRow = 0
    val columnsViolated = mutable.BitSet.empty
    var ruleIndex = 0
    while (ruleIndex < ruleCount) {
      val ruleHash = row.ruleHashes(ruleIndex)
      if (ruleHash >= 0) {
        ruleViolationsPerRow += 1
        existingReferencedColumnIndexes(ruleIndex).foreach(columnsViolated.add)
        val aggPerRule = agg.aggPerRule(ruleIndex)
        aggPerRule.violations += 1
        if (ruleHash < aggPerRule.minHash) {
          aggPerRule.minHash = ruleHash
          var i = 0
          while (i < rowsPerRule - 1 && ruleHash > aggPerRule.hashes(i + 1)) {
            aggPerRule.hashes(i) = aggPerRule.hashes(i + 1)
            aggPerRule.rowIds(i) = aggPerRule.rowIds(i + 1)
            aggPerRule.rowSample(i) = aggPerRule.rowSample(i + 1)
            aggPerRule.ruleHashes(i) = aggPerRule.ruleHashes(i + 1)
            i += 1
          }
          aggPerRule.hashes(i) = ruleHash
          aggPerRule.rowIds(i) = row.rowId
          aggPerRule.rowSample(i) = row.rowSample
          aggPerRule.ruleHashes(i) = row.ruleHashes
          if (aggPerRule.hashes.head < 0) {
            aggPerRule.minHash = Long.MaxValue
          }
        }
      }
      ruleIndex += 1
    }
    agg.aggPerTable.rows += 1
    if (ruleViolationsPerRow > 0) {
      agg.aggPerTable.violations += 1
      columnsViolated.foreach(ci => agg.aggPerColumn(ci).violations += 1)
    }
    agg
  }

  override def merge(agg1: DqAgg, agg2: DqAgg): DqAgg = {
    val merged = zero
    merged.aggPerTable.rows = agg1.aggPerTable.rows + agg2.aggPerTable.rows
    merged.aggPerTable.violations = agg1.aggPerTable.violations + agg2.aggPerTable.violations
    merged.aggPerColumn.indices.foreach { ci =>
      merged.aggPerColumn(ci).violations = agg1.aggPerColumn(ci).violations + agg2.aggPerColumn(ci).violations
    }
    merged.aggPerRule.indices.foreach { ri =>
      val aggPerRule1 = agg1.aggPerRule(ri)
      val aggPerRule2 = agg2.aggPerRule(ri)
      val aggPerRuleMerged = merged.aggPerRule(ri)
      var i = rowsPerRule - 1
      var i1 = rowsPerRule - 1
      var i2 = rowsPerRule - 1
      while (i >= 0) {
        aggPerRuleMerged.minHash = Math.min(aggPerRule1.minHash, aggPerRule2.minHash)
        if (aggPerRule1.hashes(i1) > aggPerRule2.hashes(i2)) {
          aggPerRuleMerged.hashes(i) = aggPerRule1.hashes(i1)
          aggPerRuleMerged.rowIds(i) = aggPerRule1.rowIds(i1)
          aggPerRuleMerged.rowSample(i) = aggPerRule1.rowSample(i1)
          aggPerRuleMerged.ruleHashes(i) = aggPerRule1.ruleHashes(i1)
          i1 -= 1
        } else {
          aggPerRuleMerged.hashes(i) = aggPerRule2.hashes(i2)
          aggPerRuleMerged.rowIds(i) = aggPerRule2.rowIds(i2)
          aggPerRuleMerged.rowSample(i) = aggPerRule2.rowSample(i2)
          aggPerRuleMerged.ruleHashes(i) = aggPerRule2.ruleHashes(i2)
          i2 -= 1
        }
        i -= 1
      }
      aggPerRuleMerged.violations = aggPerRule1.violations + aggPerRule2.violations
    }
    merged
  }

  override def finish(agg: DqAgg): DqAggOutputRow = {
    val rowSampleWithRuleHashesByRowId = mutable.Map.empty[Long, (Array[String], Array[Long])]
    agg.aggPerRule.foreach { aggPerRule =>
      aggPerRule.rowIds.indices.foreach { i =>
        rowSampleWithRuleHashesByRowId(aggPerRule.rowIds(i)) = (aggPerRule.rowSample(i), aggPerRule.ruleHashes(i))
      }
    }
    val rowIds = rowSampleWithRuleHashesByRowId.keys.filter(_ != -1).toArray.sorted

    DqAggOutputRow(
      rows = agg.aggPerTable.rows,
      rowViolations = agg.aggPerTable.violations,
      columnViolations = agg.aggPerColumn.map(_.violations),
      ruleViolations = agg.aggPerRule.map(_.violations),
      rowIds = rowIds,
      rowSample = rowIds.map(rowId => rowSampleWithRuleHashesByRowId(rowId)._1),
      ruleHashes = rowIds.map(rowId => rowSampleWithRuleHashesByRowId(rowId)._2)
    )
  }

  override def bufferEncoder: Encoder[DqAgg] = ExpressionEncoder[DqAgg]

  override def outputEncoder: Encoder[DqAggOutputRow] = ExpressionEncoder[DqAggOutputRow]
}

object DqAggregator {
  case class DqAggInputRow(
      rowId: Long,
      rowSample: Array[String],
      ruleHashes: Array[Long]
  )

  case class DqAggPerTable(
      var rows: Long,
      var violations: Long
  )

  case class DqAggPerRule(
      var violations: Long,
      var minHash: Long,
      var hashes: Array[Long],
      var rowIds: Array[Long],
      var rowSample: Array[Array[String]],
      var ruleHashes: Array[Array[Long]]
  )

  case class DqAggPerColumn(
      var violations: Long
  )

  case class DqAgg(
      aggPerTable: DqAggPerTable,
      aggPerColumn: Array[DqAggPerColumn],
      aggPerRule: Array[DqAggPerRule]
  )

  case class DqAggOutputRow(
      rows: Long,
      rowViolations: Long,
      columnViolations: Array[Long],
      ruleViolations: Array[Long],
      rowIds: Array[Long],
      rowSample: Array[Array[String]],
      ruleHashes: Array[Array[Long]]
  )
}
