package io.stoys.spark.aggsum

case class AggSumConfig(
    /**
     * What is the maximum number of rows in [[AggSumResult.data_json]].
     */
    limit: Option[Int],
)

object AggSumConfig {
  val default: AggSumConfig = AggSumConfig(
    limit = Some(10000),
  )
}
