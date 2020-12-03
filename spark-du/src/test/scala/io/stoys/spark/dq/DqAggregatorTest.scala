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
    val columnCount = 3
    val ruleCount = 3
    val referencedColumnIndexes = Seq(Seq(0), Seq(0), Seq(1))

    val expected = Array(DqAggOutputRow(
      rows = 4,
      rowViolations = 3,
      columnViolations = Array(2, 2, 0),
      ruleViolations = Array(0, 2, 2),
      rowIds = Array(1, 3, 4),
      rowSample = Array(0, 2, 3).map(i => input(i).rowSample),
      violatedRuleIndexes = Array(Array(1), Array(1, 2), Array(2))
    ))

    val aggregator = new DqAggregator(columnCount, ruleCount, referencedColumnIndexes, DqConfig.default)
    val actual = input.toDS().select(aggregator.toColumn).collect()
    assert(actual.length === 1)
    assert(Jackson.objectMapper.writeValueAsString(actual) === Jackson.objectMapper.writeValueAsString(expected))

    val emptyDqConfigAggregator =
      new DqAggregator(columnCount, ruleCount, referencedColumnIndexes, Arbitrary.empty[DqConfig])
    assert(input.toDS().select(emptyDqConfigAggregator.toColumn).collect().head.rowSample.length === 0)
    val singleRowSamplingAggregator =
      new DqAggregator(columnCount, ruleCount, referencedColumnIndexes, DqConfig.default.copy(max_rows_per_rule = 1))
    assert(input.toDS().select(singleRowSamplingAggregator.toColumn).collect().head.rowSample.length === 2)
  }
}
