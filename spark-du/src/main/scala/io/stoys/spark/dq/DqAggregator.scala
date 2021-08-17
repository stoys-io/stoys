package io.stoys.spark.dq

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

private[dq] class DqAggregator(columnCount: Int, ruleCount: Int, partitionCount: Int, sensibleRowNumber: Boolean,
    existingReferencedColumnIndexes: Seq[Seq[Int]], config: DqConfig)
    extends Aggregator[DqAggregator.DqAggInputRow, DqAggregator.DqAgg, DqAggregator.DqAggOutputRow] {

  import DqAggregator._

  private val isSamplingEnabled = config.sample_rows && config.max_rows_per_rule > 0 && config.max_rows > 0
  private val isRowCountingEnabled = isSamplingEnabled && sensibleRowNumber

  override def zero: DqAgg = {
    DqAgg(
      aggPerTable = DqAggPerTable(
        rows = 0L,
        violations = 0L,
        maxRowIdPerPartition = if (isRowCountingEnabled) Array.fill(partitionCount)(-1L) else null
      ),
      aggPerColumn = Array.fill(columnCount)(
        DqAggPerColumn(
          violations = 0L
        )
      ),
      aggPerRule = Array.fill(ruleCount)(
        DqAggPerRule(
          rowSample = if (isSamplingEnabled) zeroRowSample else null,
          violations = 0L
        )
      )
    )
  }

  private def zeroRowSample: DqAggRowSample = {
    DqAggRowSample(
      first = null,
      last = null,
      hashed = Array.fill(config.max_rows_per_rule)(null),
      maxHash = Long.MaxValue
    )
  }

  override def reduce(agg: DqAgg, row: DqAggInputRow): DqAgg = {
    var ruleViolationsPerRow = 0
    val columnsViolated = mutable.BitSet.empty
    var ri = 0
    while (ri < ruleCount) {
      if (row.ruleHashes(ri) >= 0) {
        ruleViolationsPerRow += 1
        existingReferencedColumnIndexes(ri).foreach(columnsViolated.add)
        val aggPerRule = agg.aggPerRule(ri)
        aggPerRule.violations += 1
        if (isSamplingEnabled) {
          reduceRowSample(aggPerRule.rowSample, row, ri)
        }
      }
      ri += 1
    }
    agg.aggPerTable.rows += 1
    if (ruleViolationsPerRow > 0) {
      agg.aggPerTable.violations += 1
      columnsViolated.foreach(ci => agg.aggPerColumn(ci).violations += 1)
    }
    if (isRowCountingEnabled) {
      reduceRowCount(agg.aggPerTable.maxRowIdPerPartition, row.rowId)
    }
    agg
  }

  private def reduceRowSample(rowSample: DqAggRowSample, row: DqAggInputRow, ri: Int): Unit = {
    if (sensibleRowNumber) {
      if (rowSample.first == null || rowSample.first.rowId > row.rowId) {
        rowSample.first = row
      }
      if (rowSample.last == null || rowSample.last.rowId < row.rowId) {
        rowSample.last = row
      }
    }
    if (row.ruleHashes(ri) < rowSample.maxHash) {
      val hashed = rowSample.hashed
      val indexOfFirstBiggerHashOrNull = hashed.indexWhere(rs => rs == null || rs.ruleHashes(ri) > row.ruleHashes(ri))
      val indexOfFirstNull = hashed.indexOf(null, indexOfFirstBiggerHashOrNull)
      val lastIndexToMoveTo = if (indexOfFirstNull >= 0) indexOfFirstNull else config.max_rows_per_rule - 1
      lastIndexToMoveTo.until(indexOfFirstBiggerHashOrNull, -1).foreach(i => hashed(i) = hashed(i - 1))
      hashed(indexOfFirstBiggerHashOrNull) = row
      if (indexOfFirstNull < 0 || indexOfFirstNull == config.max_rows_per_rule - 1) {
        rowSample.maxHash = hashed(indexOfFirstBiggerHashOrNull).ruleHashes(ri)
      }
    }
  }

  private def reduceRowCount(maxRowIdPerPartition: Array[Long], rowId: Long): Unit = {
    val partitionId = (rowId >> PARTITION_ID_BIT_OFFSET).toInt
    maxRowIdPerPartition(partitionId) = math.max(maxRowIdPerPartition(partitionId), rowId & ROW_INDEX_BIT_MASK)
  }

  override def merge(agg1: DqAgg, agg2: DqAgg): DqAgg = {
    DqAgg(
      aggPerTable = DqAggPerTable(
        rows = agg1.aggPerTable.rows + agg2.aggPerTable.rows,
        violations = agg1.aggPerTable.violations + agg2.aggPerTable.violations,
        maxRowIdPerPartition = if (isRowCountingEnabled) {
          mergeRowCount(agg1.aggPerTable.maxRowIdPerPartition, agg2.aggPerTable.maxRowIdPerPartition)
        } else {
          null
        }
      ),
      aggPerColumn = 0.until(columnCount).toArray.map { ci =>
        DqAggPerColumn(
          violations = agg1.aggPerColumn(ci).violations + agg2.aggPerColumn(ci).violations
        )
      },
      aggPerRule = 0.until(ruleCount).toArray.map { ri =>
        DqAggPerRule(
          rowSample = if (isSamplingEnabled) {
            mergeRowSample(agg1.aggPerRule(ri).rowSample, agg2.aggPerRule(ri).rowSample, ri)
          } else {
            null
          },
          violations = agg1.aggPerRule(ri).violations + agg2.aggPerRule(ri).violations
        )
      }
    )
  }

  private def mergeRowSample(rowSample1: DqAggRowSample, rowSample2: DqAggRowSample, ri: Int): DqAggRowSample = {
    val rowSample12 = Seq(rowSample1, rowSample2).filter(_ != null)
    val hashed = rowSample12.flatMap(_.hashed).filter(_ != null).sortBy(_.ruleHashes(ri))
    DqAggRowSample(
      first = rowSample12.map(_.first).filter(_ != null).sortBy(_.rowId).headOption.orNull,
      last = rowSample12.map(_.last).filter(_ != null).sortBy(_.rowId).lastOption.orNull,
      hashed = hashed.take(config.max_rows_per_rule).toArray,
      maxHash = hashed.headOption.map(_.ruleHashes(ri)).getOrElse(Long.MaxValue)
    )
  }

  private def mergeRowCount(maxRowIdPerPartition1: Array[Long], maxRowIdPerPartition2: Array[Long]): Array[Long] = {
    0.until(partitionCount).toArray.map(i => math.max(maxRowIdPerPartition1(i), maxRowIdPerPartition2(i)))
  }

  override def finish(agg: DqAgg): DqAggOutputRow = {
    val rowSample = if (isSamplingEnabled) {
      finishRowSample(agg.aggPerRule.map(_.rowSample))
    } else {
      Array.empty[DqAggInputRow]
    }
    val finalRowSample = if (isRowCountingEnabled) {
      finishRowCount(agg.aggPerTable.maxRowIdPerPartition, rowSample)
    } else {
      rowSample
    }

    DqAggOutputRow(
      rows = agg.aggPerTable.rows,
      rowViolations = agg.aggPerTable.violations,
      columnViolations = agg.aggPerColumn.map(_.violations),
      ruleViolations = agg.aggPerRule.map(_.violations),
      rowSample = finalRowSample
    )
  }

  private def finishRowSample(rowSamples: Array[DqAggRowSample]): Array[DqAggInputRow] = {
    case class Tmp(ruleIndex: Int, rowSample: Array[DqAggInputRow], last: Int)
    val flattenRowSamples = rowSamples.map(rs => (rs.first +: rs.last +: rs.hashed).filter(_ != null))
    val tmpQueue = mutable.Queue[Tmp](flattenRowSamples.zipWithIndex.map(rsi => Tmp(rsi._2, rsi._1, 0)): _*)
    val finalRowSample = mutable.Buffer.empty[DqAggInputRow]
    val seenRowIds = mutable.Set.empty[Long]
    val accepted = Array.fill(ruleCount)(0)
    while (tmpQueue.nonEmpty && finalRowSample.size < config.max_rows) {
      val tmp = tmpQueue.dequeue()
      val newRowIndex = tmp.rowSample.indexWhere(rs => !seenRowIds.contains(rs.rowId), tmp.last)
      if (newRowIndex >= 0 && accepted(tmp.ruleIndex) < config.max_rows_per_rule) {
        val newRow = tmp.rowSample(newRowIndex)
        finalRowSample += newRow
        seenRowIds.add(newRow.rowId)
        newRow.ruleHashes.zipWithIndex.filter(_._1 >= 0).foreach(rhi => accepted(rhi._2) += 1)
        tmpQueue.enqueue(tmp.copy(last = newRowIndex))
      }
    }
    finalRowSample.sortBy(_.rowId).toArray
  }

  private def finishRowCount(
      maxRowIdPerPartition: Array[Long], rowSample: Array[DqAggInputRow]): Array[DqAggInputRow] = {
    val maxRowIdPrefixSum = maxRowIdPerPartition.scanLeft(0L)((acc, maxRowId) => acc + maxRowId + 1)
    rowSample.map { rs =>
      val rowNumber = maxRowIdPrefixSum((rs.rowId >> PARTITION_ID_BIT_OFFSET).toInt) + (rs.rowId & ROW_INDEX_BIT_MASK)
      rs.copy(row = rs.row :+ rowNumber.toString)
    }
  }

  override def bufferEncoder: Encoder[DqAgg] = ExpressionEncoder[DqAgg]()

  override def outputEncoder: Encoder[DqAggOutputRow] = ExpressionEncoder[DqAggOutputRow]()
}

private[dq] object DqAggregator {
  private val PARTITION_ID_BIT_OFFSET = 33
  private val ROW_INDEX_BIT_MASK = (1L << PARTITION_ID_BIT_OFFSET) - 1

  case class DqAggInputRow(
      rowId: Long,
      row: Array[String],
      ruleHashes: Array[Long]
  )

  case class DqAggPerTable(
      var rows: Long,
      var violations: Long,
      var maxRowIdPerPartition: Array[Long]
  )

  case class DqAggRowSample(
      var first: DqAggInputRow,
      var last: DqAggInputRow,
      var hashed: Array[DqAggInputRow],
      var maxHash: Long
  )

  case class DqAggPerRule(
      var rowSample: DqAggRowSample,
      var violations: Long
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
