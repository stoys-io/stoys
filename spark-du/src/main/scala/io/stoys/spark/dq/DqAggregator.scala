package io.stoys.spark.dq

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

private[dq] class DqAggregator(columnCount: Int, existingReferencedColumnIndexes: Seq[Seq[Int]], config: DqConfig)
    extends Aggregator[DqAggregator.DqAggInputRow, DqAggregator.DqAgg, DqAggregator.DqAggOutputRow] {
  import DqAggregator._

  private val ruleCount = existingReferencedColumnIndexes.size
  private val maxRows = if (config.sample_rows) config.max_rows else 0
  private val maxRowsPerRule = if (config.sample_rows) config.max_rows_per_rule else 0
  private val defaultMaxHash = if (maxRowsPerRule <= 0) Long.MinValue else Long.MaxValue

  override def zero: DqAgg = {
    DqAgg(
      aggPerTable = DqAggPerTable(rows = 0L, violations = 0L),
      aggPerColumn = Array.fill(columnCount)(DqAggPerColumn(violations = 0L)),
      aggPerRule = Array.fill(ruleCount)(
        DqAggPerRule(
          firstRow = null,
          lastRow = null,
          rowSample = Array.fill(maxRowsPerRule)(null),
          violations = 0L,
          maxHash = defaultMaxHash
        )
      )
    )
  }

  override def reduce(agg: DqAgg, row: DqAggInputRow): DqAgg = {
    var ruleViolationsPerRow = 0
    val columnsViolated = mutable.BitSet.empty
    var ri = 0
    while (ri < ruleCount) {
      val ruleHash = row.ruleHashes(ri)
      if (ruleHash >= 0) {
        ruleViolationsPerRow += 1
        existingReferencedColumnIndexes(ri).foreach(columnsViolated.add)
        val aggPerRule = agg.aggPerRule(ri)
        aggPerRule.violations += 1
        if (aggPerRule.firstRow == null || aggPerRule.firstRow.rowId > row.rowId) {
          aggPerRule.firstRow = row
        }
        if (aggPerRule.lastRow == null || aggPerRule.lastRow.rowId < row.rowId) {
          aggPerRule.lastRow = row
        }
        if (ruleHash < aggPerRule.maxHash) {
          val rowSample = aggPerRule.rowSample
          val indexOfFirstBiggerHashOrNull = rowSample.indexWhere(rs => rs == null || rs.ruleHashes(ri) > ruleHash)
          val indexOfFirstNull = rowSample.indexOf(null, indexOfFirstBiggerHashOrNull)
          val lastIndexToMoveTo = if (indexOfFirstNull >= 0) indexOfFirstNull else maxRowsPerRule - 1
          lastIndexToMoveTo.until(indexOfFirstBiggerHashOrNull, -1).foreach(i => rowSample(i) = rowSample(i - 1))
          rowSample(indexOfFirstBiggerHashOrNull) = row
          if (indexOfFirstNull < 0 || indexOfFirstNull == maxRowsPerRule - 1) {
            aggPerRule.maxHash = rowSample(indexOfFirstBiggerHashOrNull).ruleHashes(ri)
          }
        }
      }
      ri += 1
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
      val aggPerRule12 = Seq(agg1, agg2).map(_.aggPerRule(ri))
      merged.aggPerRule(ri).firstRow = aggPerRule12.map(_.firstRow).filter(_ != null).sortBy(_.rowId).headOption.orNull
      merged.aggPerRule(ri).lastRow = aggPerRule12.map(_.lastRow).filter(_ != null).sortBy(_.rowId).lastOption.orNull
      val combinedRowSample = (agg1.aggPerRule(ri).rowSample ++ agg2.aggPerRule(ri).rowSample).filter(_ != null)
      val combinedRowSampleSorted = combinedRowSample.sortBy(_.ruleHashes(ri))
      merged.aggPerRule(ri).rowSample = combinedRowSampleSorted.take(maxRowsPerRule)
      merged.aggPerRule(ri).violations = agg1.aggPerRule(ri).violations + agg2.aggPerRule(ri).violations
      merged.aggPerRule(ri).maxHash = combinedRowSampleSorted.headOption.map(_.ruleHashes(ri)).getOrElse(defaultMaxHash)
    }
    merged
  }

  override def finish(agg: DqAgg): DqAggOutputRow = {
    case class Tmp(ruleIndex: Int, rowSample: Array[DqAggInputRow], last: Int)
    val rowSamples = agg.aggPerRule.map(apr => (apr.firstRow +: apr.lastRow +: apr.rowSample).filter(_ != null))
    val tmpQueue = rowSamples.zipWithIndex.map(rsi => Tmp(rsi._2, rsi._1, 0)).to[mutable.Queue]
    val finalRowSample = mutable.Buffer.empty[DqAggInputRow]
    val seenRowIds = mutable.Set.empty[Long]
    val accepted = Array.fill(ruleCount)(0)
    while (tmpQueue.nonEmpty && finalRowSample.size < maxRows) {
      val tmp = tmpQueue.dequeue()
      val newRowIndex = tmp.rowSample.indexWhere(rs => !seenRowIds.contains(rs.rowId), tmp.last)
      if (newRowIndex >= 0 && accepted(tmp.ruleIndex) < maxRowsPerRule) {
        val newRow = tmp.rowSample(newRowIndex)
        finalRowSample += newRow
        seenRowIds.add(newRow.rowId)
        newRow.ruleHashes.zipWithIndex.filter(_._1 >= 0).foreach(rhi => accepted(rhi._2) += 1)
        tmpQueue.enqueue(tmp.copy(last = newRowIndex))
      }
    }

    DqAggOutputRow(
      rows = agg.aggPerTable.rows,
      rowViolations = agg.aggPerTable.violations,
      columnViolations = agg.aggPerColumn.map(_.violations),
      ruleViolations = agg.aggPerRule.map(_.violations),
      rowSample = finalRowSample.sortBy(_.rowId).toArray
    )
  }

  override def bufferEncoder: Encoder[DqAgg] = ExpressionEncoder[DqAgg]()

  override def outputEncoder: Encoder[DqAggOutputRow] = ExpressionEncoder[DqAggOutputRow]()
}

private[dq] object DqAggregator {
  case class DqAggInputRow(
      rowId: Long,
      row: Array[String],
      ruleHashes: Array[Long]
  )

  case class DqAggPerTable(
      var rows: Long,
      var violations: Long
  )

  case class DqAggPerRule(
      var firstRow: DqAggInputRow,
      var lastRow: DqAggInputRow,
      var rowSample: Array[DqAggInputRow],
      var violations: Long,
      var maxHash: Long
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
      rowSample: Array[DqAggInputRow]
  )
}
