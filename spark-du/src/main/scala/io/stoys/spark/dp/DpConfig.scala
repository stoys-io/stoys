package io.stoys.spark.dp

case class DpConfig(
    buckets: Int,
    items: Int
)

object DpConfig {
  val default: DpConfig = DpConfig(
    buckets = 100,
    items = 100
  )
}
