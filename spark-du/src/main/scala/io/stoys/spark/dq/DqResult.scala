package io.stoys.spark.dq

case class DqColumn(
    name: String,
)

case class DqTableStatistic(
    rows: Long,
    violations: Long,
)

case class DqRuleStatistics(
    rule_name: String,
    violations: Long,
)

case class DqColumnStatistics(
    column_name: String,
    violations: Long,
)

case class DqStatistics(
    table: DqTableStatistic,
    column: Seq[DqColumnStatistics],
    rule: Seq[DqRuleStatistics],
)

case class DqRowSample(
    row: Seq[String],
    violated_rule_names: Seq[String],
)

case class DqResult(
    columns: Seq[DqColumn],
    rules: Seq[DqRule],
    statistics: DqStatistics,
    row_sample: Seq[DqRowSample],
    metadata: Map[String, String],
)
