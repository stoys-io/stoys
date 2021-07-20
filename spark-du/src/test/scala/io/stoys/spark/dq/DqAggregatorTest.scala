package io.stoys.spark.dq

import io.stoys.scala.{Arbitrary, Jackson}
import io.stoys.spark.test.SparkTestBase

class DqAggregatorTest extends SparkTestBase {
  import DqAggregator._
  import sparkSession.implicits._

  test("DqAggregator") {
    val input = Seq(
      DqAggInputRow(1, Array("1", "foo", "extra"), Array(-1, 12, -1)),
      DqAggInputRow(2, Array("2", "bar", "extra"), Array(-1, -1, -1)),
      DqAggInputRow(3, Array("3", "invalid", "extra"), Array(-1, 32, 33)),
      DqAggInputRow(4, Array("4", null, "extra"), Array(-1, -1, 43))
    )
    val columnCount = input.headOption.map(_.rowSample.length).getOrElse(0)
    val existingReferencedColumnIndexes = Seq(Seq(0), Seq(0), Seq(1))

    val expected = Array(DqAggOutputRow(
      rows = 4,
      rowViolations = 3,
      columnViolations = Array(2, 2, 0),
      ruleViolations = Array(0, 2, 2),
      rowIds = Array(1, 3, 4),
      rowSample = Array(0, 2, 3).map(i => input(i).rowSample),
      ruleHashes = Array(Array(-1, 12, -1), Array(-1, 32, 33), Array(-1, -1, 43))
    ))

    val aggregator = new DqAggregator(columnCount, existingReferencedColumnIndexes, DqConfig.default)
    val actual = input.toDS().select(aggregator.toColumn).collect()
    assert(Jackson.objectMapper.writeValueAsString(actual) === Jackson.objectMapper.writeValueAsString(expected))

    val emptyDqConfigAggregator =
      new DqAggregator(columnCount, existingReferencedColumnIndexes, Arbitrary.empty[DqConfig])
    assert(input.toDS().select(emptyDqConfigAggregator.toColumn).first().rowSample.length === 0)
    val singleRowSamplingAggregator =
      new DqAggregator(columnCount, existingReferencedColumnIndexes, DqConfig.default.copy(max_rows_per_rule = 1))
    assert(input.toDS().select(singleRowSamplingAggregator.toColumn).first().rowSample.length === 2)
  }
}
