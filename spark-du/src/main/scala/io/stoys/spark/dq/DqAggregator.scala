package io.stoys.spark.dq

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

private[dq] class DqAggregator(columnCount: Int, existingReferencedColumnIndexes: Seq[Seq[Int]], config: DqConfig)
    extends Aggregator[DqAggregator.DqAggInputRow, DqAggregator.DqAgg, DqAggregator.DqAggOutputRow] {
  import DqAggregator._

  private val ruleCount = existingReferencedColumnIndexes.size
  private val rowsPerRule = if (config.sample_rows) config.max_rows_per_rule else 0
  private val defaultMaxHash = if (rowsPerRule <= 0) Long.MinValue else Long.MaxValue

  override def zero: DqAgg = {
    DqAgg(
      aggPerTable = DqAggPerTable(rows = 0L, violations = 0L),
      aggPerColumn = Array.fill(columnCount)(DqAggPerColumn(violations = 0L)),
      aggPerRule = Array.fill(ruleCount)(
        DqAggPerRule(
          violations = 0L,
          maxHash = defaultMaxHash,
          rowSample = Array.fill(rowsPerRule)(null)
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
        if (ruleHash < aggPerRule.maxHash) {
          val rowSample = aggPerRule.rowSample
          val indexOfFirstBiggerHashOrNull = rowSample.indexWhere(rs => rs == null || rs.ruleHashes(ri) > ruleHash)
          val indexOfFirstNull = rowSample.indexOf(null, indexOfFirstBiggerHashOrNull)
          val lastIndexToMoveTo = if (indexOfFirstNull >= 0) indexOfFirstNull else rowsPerRule - 1
          lastIndexToMoveTo.until(indexOfFirstBiggerHashOrNull, -1).foreach(i => rowSample(i) = rowSample(i - 1))
          rowSample(indexOfFirstBiggerHashOrNull) = row.copy()
          if (indexOfFirstNull < 0 || indexOfFirstNull == rowsPerRule - 1) {
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
      val combinedRowSample = (agg1.aggPerRule(ri).rowSample ++ agg2.aggPerRule(ri).rowSample).filter(_ != null)
      val combinedRowSampleSorted = combinedRowSample.sortBy(r => Option(r).map(_.ruleHashes(ri)).getOrElse(-1L))
      merged.aggPerRule(ri).rowSample = combinedRowSampleSorted.take(rowsPerRule)
      merged.aggPerRule(ri).violations = agg1.aggPerRule(ri).violations + agg2.aggPerRule(ri).violations
      merged.aggPerRule(ri).maxHash = combinedRowSampleSorted.headOption.map(_.ruleHashes(ri)).getOrElse(defaultMaxHash)
    }
    merged
  }

  override def finish(agg: DqAgg): DqAggOutputRow = {
    val combinedRowSample = agg.aggPerRule.flatMap(_.rowSample).filter(_ != null)
    val distinctRowSample = combinedRowSample.map(rs => rs.rowId -> rs).toMap.values.toArray
    DqAggOutputRow(
      rows = agg.aggPerTable.rows,
      rowViolations = agg.aggPerTable.violations,
      columnViolations = agg.aggPerColumn.map(_.violations),
      ruleViolations = agg.aggPerRule.map(_.violations),
      rowSample = distinctRowSample.sortBy(_.rowId)
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
