package io.stoys.spark.dq

import io.stoys.scala.{Arbitrary, Jackson}
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.functions.monotonically_increasing_id

class DqAggregatorTest extends SparkTestBase {
  import DqAggregator._
  import sparkSession.implicits._

  test("DqAggregator") {
    val inputRows = Seq(
      DqAggInputRow(1, Array("1", "foo", "extra"), Array(-1, 12, -1)),
      DqAggInputRow(2, Array("2", "bar", "extra"), Array(-1, -1, -1)),
      DqAggInputRow(3, Array("3", "invalid", "extra"), Array(-1, 32, 33)),
      DqAggInputRow(4, Array("4", null, "extra"), Array(-1, -1, 43))
    )
    val inputRowsDs = inputRows.toDS()
    val columnCount = inputRows.headOption.map(_.row.length).getOrElse(0)
    val ruleCount = inputRows.headOption.map(_.ruleHashes.length).getOrElse(0)
    val partitionCount = inputRowsDs.rdd.getNumPartitions
    val existingReferencedColumnIndexes = Seq(Seq(0), Seq(0), Seq(1))

    val expected = DqAggOutputRow(
      rows = 4,
      rowViolations = 3,
      columnViolations = Array(2, 2, 0),
      ruleViolations = Array(0, 2, 2),
      rowSample = inputRows.filter(_.ruleHashes.exists(_ != -1)).toArray
    )

    val aggregator = new DqAggregator(columnCount, ruleCount, partitionCount,
      sensibleRowNumber = false, existingReferencedColumnIndexes, DqConfig.default)
    val actual = inputRowsDs.select(aggregator.toColumn).first()
    assert(Jackson.json.writeValueAsString(actual) === Jackson.json.writeValueAsString(expected))

    val emptyDqConfigAggregator = new DqAggregator(columnCount, ruleCount, partitionCount,
      sensibleRowNumber = false, existingReferencedColumnIndexes, Arbitrary.empty[DqConfig])
    assert(inputRowsDs.select(emptyDqConfigAggregator.toColumn).first().rowSample.length === 0)
    val singleRowSamplingAggregator = new DqAggregator(columnCount, ruleCount, partitionCount,
      sensibleRowNumber = false, existingReferencedColumnIndexes, DqConfig.default.copy(max_rows_per_rule = 1))
    assert(inputRowsDs.select(singleRowSamplingAggregator.toColumn).first().rowSample.length === 2)
  }

  test("DqAggregator - sampling") {
    val rowIds = 0.until(8)
    val secondRuleHashes = Array(-1L, -1L, 4L, 3L, 2L, 1L, -1L, -1L)
    val inputRows = rowIds.map(i => DqAggInputRow(i, Array(i.toString, "foo"), Array(i, secondRuleHashes(i))))
    val inputRowsDs = inputRows.toDS().repartition(3)
    val columnCount = inputRows.headOption.map(_.row.length).getOrElse(0)
    val ruleCount = inputRows.headOption.map(_.ruleHashes.length).getOrElse(0)
    val partitionCount = inputRowsDs.rdd.getNumPartitions
    val existingReferencedColumnIndexes = Seq(Seq(0), Seq(0, 1))

    def run(config: DqConfig): Seq[Int] = {
      val aggregator = new DqAggregator(columnCount, ruleCount, partitionCount,
        sensibleRowNumber = false, existingReferencedColumnIndexes, config)
      inputRowsDs.select(aggregator.toColumn).first().rowSample.map(_.row.head.toInt)
    }

    assert(run(DqConfig.default) === rowIds)
    assert(run(DqConfig.default.copy(sample_rows = false)) === Seq.empty)

    assert(run(DqConfig.default.copy(max_rows_per_rule = 0, max_rows = 9)) === Seq.empty)
    assert(run(DqConfig.default.copy(max_rows_per_rule = 1, max_rows = 0)) === Seq.empty)
    assert(run(DqConfig.default.copy(max_rows_per_rule = 1, max_rows = 9)) === Seq(0, 5))
    assert(run(DqConfig.default.copy(max_rows_per_rule = 2, max_rows = 9)) === Seq(0, 4, 5))
    assert(run(DqConfig.default.copy(max_rows_per_rule = 2, max_rows = 2)) === Seq(0, 5))
    assert(run(DqConfig.default.copy(max_rows_per_rule = 3, max_rows = 9)) === Seq(0, 1, 3, 4, 5))
    assert(run(DqConfig.default.copy(max_rows_per_rule = 3, max_rows = 3)) === Seq(0, 1, 5))
    assert(run(DqConfig.default.copy(max_rows_per_rule = 4, max_rows = 9)) === Seq(0, 1, 2, 3, 4, 5))
    assert(run(DqConfig.default.copy(max_rows_per_rule = 9, max_rows = 9)) === rowIds)
  }

  test("DqAggregator - counting") {
    val rowIds = 0.until(8)
    val inputRowsDs = rowIds.map(i => DqAggInputRow(-1L, Array(i.toString), Array(i))).toDS()

    def run(sensibleRowNumber: Boolean, partitions: Int): DqAggOutputRow = {
      val patchedInputRowDf = inputRowsDs.repartition(partitions).withColumn("rowId", monotonically_increasing_id())
      val aggregator = new DqAggregator(columnCount = 1, ruleCount = 1, partitionCount = partitions,
        sensibleRowNumber = sensibleRowNumber, existingReferencedColumnIndexes = Seq(Seq.empty), DqConfig.default)
      patchedInputRowDf.as[DqAggInputRow].select(aggregator.toColumn).first()
    }

    assert(run(sensibleRowNumber = true, partitions = 1).rowSample.map(_.row.last.toInt) === rowIds)
    assert(run(sensibleRowNumber = true, partitions = 3).rowSample.map(_.row.last.toInt) === rowIds)
    assert(run(sensibleRowNumber = false, partitions = 3).rowSample.map(_.row.last.toInt) !== rowIds)
  }
}
